import io
from collections import namedtuple

from ruamel.yaml import YAML
import typing


def yaml_loads(yaml_text: str) -> typing.Any:
    return YAML(typ="safe").load(yaml_text)


def yaml_dumps(obj: typing.Any) -> str:
    output = io.StringIO()
    YAML().dump(obj, output)
    output.seek(0)
    return output.read()


# yaml.loads(str) -> obj, yaml.dumps(obj) -> str
yaml = namedtuple("YAML", ("loads", "dumps"))(yaml_loads, yaml_dumps)
