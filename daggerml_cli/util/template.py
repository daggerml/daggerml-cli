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

class Ref(edn.TaggedElement):
    def __init__(self, namespace, name):
        self.namespace = namespace
        self.name = name
    def __str__(self):
        ns = dissoc(self.namespace.form, Keyword('as'))
        ns = assoc(ns, Keyword('name'), self.name)
        return f"#ref {edn.dumps(ns)}"

@edn.tag('import')
class Namespace(edn.TaggedElement):
    def __init__(self, form={}):
        self.form = form
        self.alias = form.get(Keyword('as'))
    def ref(self, name):
        return Ref(self, name)
    def __str__(self):
        return f"#import {edn.dumps(self.form)}"

class Node(edn.TaggedElement):
    def __init__(self, func, args):
        self.form = {
            Keyword('name'): gensym(),
            Keyword('func'): func,
            Keyword('args'): args,
        }
    def __str__(self):
        return f"#node {edn.dumps(self.form)}"
    def ref(self, ns):
        return Ref(ns, self.form[Keyword('name')])

class Def(edn.TaggedElement):
    def __init__(self, ns, name, ref):
        self.form = {
            Keyword('ns'): ns,
            Keyword('name'): name,
            Keyword('ref'): ref,
        }
    def __str__(self):
        return f"#def {edn.dumps(self.form)}"

class Dag:
    def __init__(self, template_file, ns):
        self.specials = {
            Symbol('arg'): self.special_dummy,
            Symbol('dag'): self.special_dummy,
            Symbol('def'): self.special_def
        }
        self.ns = ns
        self.aliases = {}
        self.exprs = []
        for expr in parse_edn(template_file):
            if isinstance(expr, Namespace):
                self.aliases[expr.alias.name] = expr
            else:
                self.exprs += (self.analyze(expr) or [])

    def resolve_sym(self, sym):
        parts = sym.name.split('/')
        ns = self.aliases[parts[0]] if len(parts) == 2 else self.ns
        return ns.ref(parts[-1])

    def resolve_arg(self, arg):
        return arg if isinstance(arg, Ref) else arg.ref(self.ns)

    def special_dummy(self, x):
        return [(Symbol(f"SPECIAL::{x[0].name}"), *x[1:])]

    def special_def(self, x):
        name = x[1]
        ns = self.analyze(x[2])
        ref = self.resolve_arg(ns[-1])
        return ns + [Def(self.ns, name, ref)]

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
            return nodes + [Node(self.resolve_sym(x[0]), args)]

    def analyze(self, expr):
        ret = (
            self.analyze_special(expr) or
            self.analyze_self_evaluating(expr) or
            self.analyze_reference(expr) or
            self.analyze_func_application(expr) or
            list(expr)
        )
        #print('EXPR', edn.dumps(expr))
        #print('ANAL', edn.dumps(ret))
        return ret

def parse(template_file):
    dag = Dag(
        template_file,
        Namespace({
            Keyword('from'): 'foo.bar.baz',
            Keyword('version'): 'v0.1.0',
            Keyword('as'): None
        })
    )
    for expr in dag.exprs:
        print(edn.dumps(expr))

    return None
