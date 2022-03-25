import edn_format as edn
from uuid import uuid4
from edn_format import (
    ImmutableList,
    ImmutableDict,
    Keyword,
    Symbol,
    Char,
    TaggedElement,
    EDNDecodeError
)

edn_indent = 0

def parse_edn(file):
    with open(file, "r") as f:
        return edn.loads_all(f.read())

def assoc(xs, k, v):
    xs = dict(xs)
    xs[k] = v
    return xs

def dissoc(xs, k):
    xs = dict(xs)
    if k in xs:
        del xs[k]
    return xs

def gensym(prefix='ref'):
    return f"{prefix}-{uuid4().hex}"

def sym_split(sym):
    parts = sym.name.split('/')
    return [parts[0], '/'.join(parts[1:])] if len(parts) > 1 else [None, parts[0]]

def sym_ns(sym):
    return sym_split(sym)[0]

def sym_name(sym):
    return sym_split(sym)[1]

class Empty:
    pass

@edn.tag('ns')
class Ns(edn.TaggedElement):
    @classmethod
    def new(cls, sym):
        return cls({
            Keyword('name'): sym_ns(sym),
            Keyword('version'): sym_name(sym),
        })
    def __init__(self, form={}):
        self.form = form
    def __str__(self):
        return f"#ns {edn.dumps(self.form)}"
    def ref(self, name):
        return Ref.new(self, name)

@edn.tag('ref')
class Ref(edn.TaggedElement):
    @classmethod
    def new(cls, ns, name):
        if isinstance(name, Symbol):
            name = name.name
        return cls({
            Keyword('ns'): ns,
            Keyword('name'): name,
        })
    def __init__(self, form):
        self.form = form
    def __str__(self):
        return f"#ref {edn.dumps(self.form)}"

@edn.tag('node')
class Node(edn.TaggedElement):
    @classmethod
    def new(cls, func, args, ns):
        return cls({
            Keyword('var'): Ref.new(ns, gensym()),
            Keyword('func'): func,
            Keyword('args'): args,
        })
    def __init__(self, form):
        self.form = form
        self.ref = form[Keyword('var')]
    def __str__(self):
        return f"#node {edn.dumps(self.form)}"

@edn.tag('def')
class Def(edn.TaggedElement):
    @classmethod
    def new(cls, var, val):
        return cls({
            Keyword('var'): var,
            Keyword('val'): val,
        })
    def __init__(self, form):
        self.form = form
    def __str__(self):
        return f"#def {edn.dumps(self.form)}"

@edn.tag('import')
class Import(edn.TaggedElement):
    @classmethod
    def new(cls, ns, alias):
        return cls({
            Keyword('ns'): ns,
            Keyword('as'): alias,
        })
    def __init__(self, form):
        self.form = form
        self.ns = form[Keyword('ns')]
        self.alias = form[Keyword('as')]
    def __str__(self):
        return f"#import {edn.dumps(self.form)}"

@edn.tag('dag')
class Dag(edn.TaggedElement):
    @classmethod
    def new(cls, template_file, ns):
        dag = cls({
            Keyword('params'): [],
            Keyword('exprs'): [],
        })
        dag.analyze_file(template_file, ns)
        return dag

    def __init__(self, form):
        self.form = form

    def __str__(self):
        return f"#dag {edn.dumps(self.form)}"

    def resolve_sym(self, sym):
        s = sym_split(sym)
        ns = self.aliases[s[0]] if s[0] else self.ns
        return ns.ref(s[1])

    def resolve_arg(self, arg):
        return arg if isinstance(arg, Ref) else arg.ref

    def special_dummy(self, x):
        return [(Symbol(f"SPECIAL::{x[0].name}"), *x[1:])]

    def special_import(self, x):
        self.aliases[x[2].name] = Ns.new(x[1])
        return [Empty()]

    def special_def(self, x):
        name = x[1]
        ns = self.analyze(x[2])
        ref = self.resolve_arg(ns[-1])
        return ns + [Def.new(self.ns.ref(name.name), ref)]

    def analyze_special(self, x):
        if isinstance(x, (ImmutableList,tuple)) and len(x) and x[0] in self.specials:
            return self.specials[x[0]](x)

    def analyze_self_evaluating(self, x):
        if isinstance(x, (Keyword, str)):
            return [x]

    def analyze_reference(self, x):
        if isinstance(x, Symbol):
            return [self.resolve_sym(x)]

    def analyze_func_application(self, x):
        if isinstance(x, (tuple, list)) and len(x) and isinstance(x[0], Symbol):
            nodes = []
            args = []
            for arg in x[1:]:
                ns = self.analyze(arg)
                nodes += [n for n in ns if isinstance(n, Node)]
                args += [self.resolve_arg(ns[-1])]
            return nodes + [Node.new(self.resolve_sym(x[0]), args, self.ns)]

    def analyze(self, expr):
        ret = (
            self.analyze_special(expr) or
            self.analyze_self_evaluating(expr) or
            self.analyze_reference(expr) or
            self.analyze_func_application(expr) or
            list(expr)
        )
        return ret

    def analyze_file(self, template_file, ns):
        self.specials = {
            Symbol('arg'): self.special_dummy,
            Symbol('dag'): self.special_dummy,
            Symbol('def'): self.special_def,
            Symbol('import'): self.special_import,
        }
        self.ns = ns
        self.aliases = {}
        self.form[Keyword('exprs')] = []
        for expr in parse_edn(template_file):
            xs = self.analyze(expr) or []
            self.form[Keyword('exprs')] += (x for x in xs if not isinstance(x, Empty))

def parse(template_file):
    dag = Dag.new(template_file, Ns.new(Symbol('foo.bar.baz/v0.1.0')))
    print(edn.dumps(dag))
    return None
