import yaml
import json

def with_open(file, mode, f):
    fh = open(file, mode)
    try:
        return f(fh)
    except Exception as e:
        print(e)
    finally:
        fh.close()

def map_vals(f, xs):
    ret = {}
    for k in xs.keys():
        ret[k] = f(xs[k])
    return ret

def map_list(f, xs):
    ret = []
    for x in xs:
        ret.append(f(x))
    return ret

class Parser:
    def __init__(self):
        class SafeLoader(yaml.SafeLoader):
            pass
        def tag_constructor(loader, suffix, node):
            k = self.tag_to_key(suffix)
            if not self.key_to_method(k):
                raise Exception(f"unknown tag: !{suffix}")
            return {k: getattr(loader, f"construct_{node.id}")(node)}
        yaml.add_multi_constructor("!", tag_constructor, Loader=SafeLoader)
        self.tag_prefix = "Fn::"
        self.loader = SafeLoader

    def tag_to_key(self, tag):
        return self.tag_prefix + tag

    def key_to_method_name(self, key):
        return key.replace(":", "_")

    def key_to_method(self, key):
        try:
            return getattr(self, self.key_to_method_name(key))
        except Exception:
            pass

    def eval_tag_fn(self, xs):
        if type(xs) is dict and len(xs.keys()) == 1:
            tag = list(xs.keys())[0]
            f = self.key_to_method(tag)
            if f:
                return lambda x: f(tag, list(x.values())[0])

    def walk(self, xs):
        tag_fn = self.eval_tag_fn(xs)
        walk_fn = lambda x: self.walk(x)
        if tag_fn:
            return tag_fn(xs)
        elif type(xs) is dict:
            return map_vals(walk_fn, xs)
        elif type(xs) is list:
            return map_list(walk_fn, xs)
        else:
            return xs

    def read_json(self, template_file):
        return with_open(template_file, "r", lambda x: json.loads(x))

    def read_yaml(self, template_file):
        return with_open(template_file, "r", lambda x: yaml.load(x, Loader=self.loader))

    def parse(self, template_file):
        if template_file.endswith(".yml"):
            doc = self.read_yaml(template_file)
        elif template_file.endswith(".json"):
            doc = self.read_json(template_file)
        else:
            raise Exception("template filename must have .json or .yml extension")
        return self.walk(doc)
