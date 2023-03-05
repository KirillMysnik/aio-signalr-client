from dataclasses import dataclass
from enum import IntEnum
from typing import Any


class MessageType(IntEnum):
    INVOCATION = 1
    STREAM_ITEM = 2
    COMPLETION = 3
    STREAM_INVOCATION = 4
    CANCEL_INVOCATION = 5
    PING = 6
    CLOSE = 7


class SignalRMessage:
    type: MessageType

    def to_dict(self) -> Any:
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data) -> 'SignalRMessage':
        raise NotImplementedError


@dataclass(kw_only=True)
class UnknownMessage(SignalRMessage):
    type: MessageType
    data: Any

    def to_dict(self) -> Any:
        return {
            'type': self.type,
            **self.data,
        }

    @classmethod
    def from_dict(cls, data) -> 'UnknownMessage':
        data_ = data.copy()
        message_type = MessageType(data_.pop('type'))
        return cls(type=message_type, data=data_)


@dataclass(kw_only=True)
class InvocationMessage(SignalRMessage):
    target: str
    arguments: list[Any]
    invocation_id: str | None

    @property
    def type(self) -> MessageType:
        return MessageType.INVOCATION

    def to_dict(self) -> Any:
        data = {
            'type': self.type,
            'target': self.target,
            'arguments': self.arguments,
        }
        if self.invocation_id is not None:
            data['invocationId'] = self.invocation_id

        return data

    @classmethod
    def from_dict(cls, data: Any) -> 'InvocationMessage':
        return cls(target=data['target'], arguments=data['arguments'], invocation_id=data.get('invocationId'))


@dataclass(kw_only=True)
class CompletionMessage(SignalRMessage):
    invocation_id: str
    result: Any | None
    error: Any | None

    @property
    def type(self) -> MessageType:
        return MessageType.COMPLETION

    def to_dict(self) -> Any:
        data = {
            'type': self.type,
            'invocationId': self.invocation_id,
        }
        if self.error is not None:
            data['error'] = self.error
        elif self.result is not None:
            data['result'] = self.result

        return data

    @classmethod
    def from_dict(cls, data: Any) -> 'CompletionMessage':
        return cls(invocation_id=data.get('invocationId'), result=data.get('result'), error=data.get('error'))
