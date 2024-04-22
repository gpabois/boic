from docxtpl import DocxTemplate
from boic.jewel import JewelPath, Jewel
from boic.shards import Shard

from jinja2 import Template

def new_shard_template(jewel: Jewel, name: str, output: JewelPath, **args) -> Shard:
    tpl_path = jewel.path(jewel.config.templates.dir.shards, f"{name}.md.tpl")
    
    with tpl_path.open() as file:
        tpl = Template(file.read())
        out = tpl.render(**args)
    
    with output.open(mode="w") as file:
        file.write(out)

def new_docx_template(jewel: Jewel, name: str, output: JewelPath, **args) -> DocxTemplate:
    tpl_path = jewel.path(jewel.config.templates.dir.documents, f"{name}.docx").canonicalize()
    
    tpl = DocxTemplate(tpl_path)
    tpl.render(args)
    
    output = output.canonicalize()
    tpl.save(output)
