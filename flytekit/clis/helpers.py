from typing import Tuple

import six as _six
from flyteidl.core import identifier_pb2 as _identifier_pb2
from flyteidl.core import workflow_pb2 as _workflow_pb2
from google.protobuf.pyext.cpp_message import GeneratedProtocolMessageType as _GeneratedProtocolMessageType

from flytekit.common.types.helpers import get_sdk_type_from_literal_type as _get_sdk_type_from_literal_type
from flytekit.models import literals as _literals


def construct_literal_map_from_variable_map(variable_dict, text_args):
    """
    This function produces a map of Literals to use when creating an execution.  It reads the required values from
    a Variable map (presumably obtained from a launch plan), and then fills in the necessary inputs
    from the click args. Click args will be strings, which will be parsed into their SDK types with each
    SDK type's parse string method.

    :param dict[Text, flytekit.models.interface.Variable] variable_dict:
    :param dict[Text, Text] text_args:
    :rtype: flytekit.models.literals.LiteralMap
    """
    inputs = {}

    for var_name, variable in _six.iteritems(variable_dict):
        # Check to see if it's passed from click
        # If this is an input that has a default from the LP, it should've already been parsed into a string,
        # and inserted into the default for this option, so it should still be here.
        if var_name in text_args and text_args[var_name] is not None:
            # the SDK type is also available from the sdk workflow object's saved user inputs but let's
            # derive it here from the interface to be more rigorous.
            sdk_type = _get_sdk_type_from_literal_type(variable.type)
            inputs[var_name] = sdk_type.from_string(text_args[var_name])

    return _literals.LiteralMap(literals=inputs)


def parse_args_into_dict(input_arguments):
    """
    Takes a tuple like (u'input_b=mystr', u'input_c=18') and returns a dictionary of input name to the
    original string value

    :param Tuple[Text] input_arguments:
    :rtype: dict[Text, Text]
    """

    return {split_arg[0]: split_arg[1] for split_arg in [input_arg.split("=", 1) for input_arg in input_arguments]}


def construct_literal_map_from_parameter_map(parameter_map, text_args):
    """
    Take a dictionary of Text to Text and construct a literal map using a ParameterMap as guidance.
    Required input parameters must have an entry in the text arguments given.
    Parameters with defaults will have those defaults filled in if missing from the text arguments.

    :param flytekit.models.interface.ParameterMap parameter_map:
    :param dict[Text, Text] text_args:
    :rtype: flytekit.models.literals.LiteralMap
    """

    # This function can be written by calling construct_literal_map_from_variable_map also, but not that much
    # code is saved.
    inputs = {}
    for var_name, parameter in _six.iteritems(parameter_map.parameters):
        sdk_type = _get_sdk_type_from_literal_type(parameter.var.type)
        if parameter.required:
            if var_name in text_args and text_args[var_name] is not None:
                inputs[var_name] = sdk_type.from_string(text_args[var_name])
            else:
                raise Exception("Missing required parameter {}".format(var_name))
        else:
            if var_name in text_args and text_args[var_name] is not None:
                inputs[var_name] = sdk_type.from_string(text_args[var_name])
            else:
                inputs[var_name] = parameter.default

    return _literals.LiteralMap(literals=inputs)


def str2bool(str):
    """
    bool('False') is True in Python, so we need to do some string parsing.  Use the same words in ConfigParser
    :param Text str:
    :rtype: bool
    """
    return not str.lower() in ["false", "0", "off", "no"]


def _hydrate_identifier(
    project: str, domain: str, version: str, identifier: _identifier_pb2.Identifier
) -> _identifier_pb2.Identifier:
    identifier.project = identifier.project or project
    identifier.domain = identifier.domain or domain
    identifier.version = identifier.version or version
    return identifier


def _hydrate_workflow_template(
    project: str, domain: str, version: str, template: _workflow_pb2.WorkflowTemplate
) -> _workflow_pb2.WorkflowTemplate:
    refreshed_nodes = []
    for node in template.nodes:
        if node.HasField("task_node"):
            task_node = node.task_node
            task_node.reference_id.CopyFrom(_hydrate_identifier(project, domain, version, task_node.reference_id))
            node.task_node.CopyFrom(task_node)
        elif node.HasField("workflow_node"):
            workflow_node = node.workflow_node
            if workflow_node.HasField("launchplan_ref"):
                workflow_node.launchplan_ref.CopyFrom(
                    _hydrate_identifier(project, domain, version, workflow_node.launchplan_ref)
                )
            elif workflow_node.HasField("sub_workflow_ref"):
                workflow_node.sub_workflow_ref.CopyFrom(
                    _hydrate_identifier(project, domain, version, workflow_node.sub_workflow_ref)
                )
            node.workflow_node.CopyFrom(workflow_node)
        refreshed_nodes.append(node)
    # Reassign nodes with the newly hydrated ones.
    del template.nodes[:]
    template.nodes.extend(refreshed_nodes)
    return template


def hydrate_registration_parameters(
    identifier: _identifier_pb2.Identifier,
    project: str,
    domain: str,
    version: str,
    entity: _GeneratedProtocolMessageType,
) -> Tuple[_identifier_pb2.Identifier, _GeneratedProtocolMessageType]:
    """
    This is called at registration time to fill out identifier fields (e.g. project, domain, version) that are mutable.
    Entity is one of \b
    - flyteidl.admin.launch_plan_pb2.LaunchPlanSpec for launch plans\n
    - flyteidl.admin.workflow_pb2.WorkflowSpec for workflows\n
    - flyteidl.admin.task_pb2.TaskSpec for tasks\n
    """
    identifier = _hydrate_identifier(project, domain, version, identifier)

    if identifier.resource_type == _identifier_pb2.LAUNCH_PLAN:
        entity.workflow_id.CopyFrom(_hydrate_identifier(project, domain, version, entity.workflow_id))
        return identifier, entity

    entity.template.id.CopyFrom(identifier)
    if identifier.resource_type == _identifier_pb2.TASK:
        return identifier, entity

    # Workflows (the only possible entity type at this point) are a little more complicated.
    # Workflow nodes that are defined inline with the workflows will be missing project/domain/version so we fill those
    # in now.
    # (entity is of type flyteidl.admin.workflow_pb2.WorkflowSpec)
    refreshed_sub_workflows = []
    for sub_workflow in entity.sub_workflows:
        refreshed_sub_workflow = _hydrate_workflow_template(project, domain, version, sub_workflow)
        refreshed_sub_workflows.append(refreshed_sub_workflow)
    # Reassign subworkflows with the newly hydrated ones.
    del entity.sub_workflows[:]
    entity.sub_workflows.extend(refreshed_sub_workflows)
    return identifier, entity
