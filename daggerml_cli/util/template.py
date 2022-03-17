from daggerml_cli.util.template_parser import Parser

class TemplateParser(Parser):
    def Fn__Ref(self, tag, form):
        return {tag: form}

def parse(template_file):
    return TemplateParser().parse(template_file)
