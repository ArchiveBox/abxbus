from abxbus.base_event import EventResult

__all__ = ['EventResult']

# EventResult cannot be defined in a separate file from BaseEvent
# because Pydantic needs to be able to reference BaseEvent and vice versa in the same file.
# This is a known issue with Pydantic and generic models:
# https://github.com/pydantic/pydantic/issues/1873
# https://github.com/pydantic/pydantic/issues/707
# https://stackoverflow.com/questions/77582955/how-can-i-separate-two-pydantic-models-into-different-files-when-these-models-ha
# https://github.com/pydantic/pydantic/issues/11532
