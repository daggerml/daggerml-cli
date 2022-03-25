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

def parse_file(file):
    with open(file, "r") as f:
        return edn.loads_all(f.read())

def wrap_do(exprs):
    return (Symbol('do'),) + tuple(exprs)

def assoc(xs, k, v):
    xs = dict(xs)
    xs[k] = v
    return xs

def dissoc(xs, k):
    xs = dict(xs)
    if k in xs:
        del xs[k]
    return xs

def gensym(prefix):
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
    def name(self):
        return self.form[Keyword('name')]
    def version(self):
        return self.form[Keyword('version')]
    def ref(self, name):
        return Ref.new(self, name)
    def sym(self):
        return Symbol(f"{self.name()}/{self.version()}")

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
    def ns(self):
        return self.form[Keyword('ns')]
    def name(self):
        return self.form[Keyword('name')]
    def ref(self):
        return self

@edn.tag('node')
class Node(edn.TaggedElement):
    @classmethod
    def new(cls, func, args, ns):
        return cls({
            Keyword('var'): Ref.new(ns, gensym('n')),
            Keyword('func'): func,
            Keyword('args'): args,
        })
    def __init__(self, form):
        self.form = form
    def __str__(self):
        return f"#node {edn.dumps(self.form)}"
    def var(self):
        return self.form[Keyword('var')]
    def func(self):
        return self.form[Keyword('func')]
    def args(self):
        return self.form[Keyword('args')]
    def ref(self):
        return self.var()

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
    def var(self):
        return self.form[Keyword('var')]
    def val(self):
        return self.form[Keyword('val')]
    def ref(self):
        return self.var()

@edn.tag('literal')
class Literal(edn.TaggedElement):
    @classmethod
    def new(cls, val, ns):
        return cls({
            Keyword('var'): Ref.new(ns, gensym('l')),
            Keyword('val'): val,
        })
    def __init__(self, form):
        self.form = form
    def __str__(self):
        return f"#literal {edn.dumps(self.form)}"
    def var(self):
        return self.form[Keyword('var')]
    def val(self):
        return self.form[Keyword('val')]
    def ref(self):
        return self.var()

@edn.tag('quote')
class Quote(edn.TaggedElement):
    @classmethod
    def new(cls, form):
        return cls(form)
    def __init__(self, form):
        self.form = form
    def __str__(self):
        return f"#quote {edn.dumps(self.form)}"

@edn.tag('dag')
class Dag(edn.TaggedElement):
    @classmethod
    def new(cls, expr, args):
        dag = cls([])
        return cls(dag.analyze(expr, args))

    def __init__(self, form):
        self.form = form
        self.specials = {
            Symbol('arg'): self.special_arg,
            Symbol('dag'): self.special_dag,
            Symbol('def'): self.special_def,
            Symbol('do'): self.special_do,
            Symbol('import'): self.special_import,
            Symbol('ns'): self.special_ns,
        }
        self.ref = None
        self.ns = None
        self.args = {}
        self.aliases = {}

    def __str__(self):
        return f"#dag {edn.dumps(self.form)}"

    ###########################################################################

    def args(self):
        return self.form[Keyword('args')]

    def exprs(self):
        return self.form[Keyword('exprs')]

    ###########################################################################

    def resolve_sym(self, sym):
        s = sym_split(sym)
        ns = self.aliases[s[0]] if s[0] else self.ns
        return ns.ref(s[1])

    ###########################################################################

    def emit_ns(self):
        return [(Symbol('ns'), self.ns.sym())]

    def emit_imports(self):
        ret = []
        for k, v in self.aliases.items():
            ret += [(Symbol('import'), v.sym(), Symbol(k))]
        return ret

    def emit_prelude(self):
        return self.emit_ns() + self.emit_imports()

    def emit_dag(self, exprs):
        return wrap_do(self.emit_prelude() + list(exprs))

    ###########################################################################

    def special_arg(self, x):
        exprs = self.analyze(self.args[x[1]] if x[1] in self.args else x[2])
        return (exprs + [Def.new(self.ns.ref(x[1].name), exprs[-1].ref())])

    def special_dag(self, x):
        return [Literal.new(Quote.new(self.emit_dag(x[1:])), self.ns)]

    def special_def(self, x):
        name = x[1]
        ns = self.analyze(x[2])
        return ns + [Def.new(self.ns.ref(name.name), ns[-1].ref())]

    def special_do(self, x):
        ret = []
        for y in x[1:]:
            ret += (self.analyze(y) or [])
        return ret

    def special_dummy(self, x):
        return [(Symbol(f"SPECIAL::{x[0].name}"), *x[1:])]

    def special_import(self, x):
        self.aliases[x[2].name] = Ns.new(x[1])
        return [Empty()]

    def special_ns(self, x):
        self.ns = Ns.new(x[1])
        return [Empty()]

    ###########################################################################

    def analyze_func_application(self, x):
        if isinstance(x, (tuple, list)) and len(x) and isinstance(x[0], Symbol):
            nodes = []
            args = []
            for arg in x[1:]:
                ns = self.analyze(arg)
                nodes += [n for n in ns if isinstance(n, Node)]
                args += [ns[-1].ref()]
            return nodes + [Node.new(self.resolve_sym(x[0]), args, self.ns)]

    def analyze_literal(self, x):
        if isinstance(x, str):
            return [Literal.new(x, self.ns)]

    def analyze_reference(self, x):
        if isinstance(x, Symbol):
            return [self.resolve_sym(x)]

    def analyze_self_evaluating(self, x):
        if isinstance(x, Keyword):
            return [x]

    def analyze_special(self, x):
        if isinstance(x, (ImmutableList,tuple)) and len(x) and x[0] in self.specials:
            return self.specials[x[0]](x)

    ###########################################################################

    def analyze(self, expr, args=None):
        if args:
            self.args = args
        ret = (
            self.analyze_special(expr) or
            self.analyze_self_evaluating(expr) or
            self.analyze_reference(expr) or
            self.analyze_literal(expr) or
            self.analyze_func_application(expr) or
            list(expr)
        )
        ret = list(x for x in ret if not isinstance(x, Empty))
        return ret

def parse(template_file, args):
    exprs = wrap_do(parse_file(template_file))
    args = edn.loads(args)
    dag = Dag.new(exprs, args)
    print(edn.dumps(dag))
    return None
