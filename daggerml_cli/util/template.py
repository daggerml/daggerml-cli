import edn_format as edn

@edn.tag("dag")
def parse_dag(form):
    return form

@edn.tag("dep")
def parse_dep(form):
    return form

@edn.tag("def")
def parse_def(form):
    return form

def parse(template_file):
    with open(template_file, "r") as f:
        text = f.read()
    doc = edn.loads_all(text)
    print(doc)
    for x in doc:
        print(edn.dumps(x))
    return None
