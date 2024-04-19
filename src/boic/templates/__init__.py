from docxtpl import DocxTemplate
from boic.jewel import JewelPath, Jewel
from boic.shards import Shard

from jinja2 import Template

def new_shard_template(jewel: Jewel, name: str, output: JewelPath, **args) -> Shard:
    with jewel.shard_templates().join(f"{name}.md.tpl").open() as file:
        tpl = Template(file.read())
        out = tpl.render(**args)
    
    with output.open(mode="w") as file:
        file.write(out)

def new_docx_template(jewel: Jewel, name: str, output: JewelPath, **args) -> DocxTemplate:
    ipt = jewel.docx_templates().join(f"{name}.docx").canonicalize()
    tpl = DocxTemplate(ipt)
    tpl.render(args)
    
    output = output.canonicalize()
    tpl.save(output)
