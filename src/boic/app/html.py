from __future__ import annotations
from typing import Union
from collections.abc import Iterator
import itertools

class Attributes:
    def __init__(self, props: dict):
        self.props = props

    def write_to_stream(self, stream):
        if not self.props:
            return
        
        for k, v in self.props.items():
            stream.write(f' {k}="{v}"'.encode("utf8"))

class Node:
    def __init__(self, tag: str, *children: Iterable[Iterator[Union[Node, str]]]):
        self.tag = tag
        self.children = list(map(Node.ensure, itertools.chain(*children)))

    def ensure(node_or_str: Union[Node, str]) -> Node:
        if isinstance(node_or_str, str):
            return Text(node_or_str)
        else:
            return node_or_str

class Text(Node):
    def __init__(self, text: str):
        super().__init__(tag="#text")
        self.text = text   

    def write_to_stream(self, stream):
        stream.write(self.text.encode("utf8"))

class Element(Node):
    def __init__(self, tag: str, props: dict, *children: Iterable[Iterator[Union[Node, str]]]):
        super().__init__(tag, *children)
        self.props: Attributes = Attributes(props)

    def write_to_stream(self, stream):
        """ Ecris le noeud dans le flux. """
        
        stream.write(f'<{self.tag}'.encode("utf8"))
        self.props.write_to_stream(stream)
        
        if len(self.children) > 0:
            stream.write(b'>')
            for c in self.children: c.write_to_stream(stream)
            stream.write(f'</{self.tag}>'.encode("utf8"))
        else:
            stream.write(b'/>')       

def e(tag: str, props: dict[str, any], *children: Iterable[Iterator[Union[Node, str]]]) -> Element:
    return Element(tag, props, *children)