# coding: utf-8

import json
from collections import Mapping

import xmltodict

from .exceptions import (
    ResponseProcessException,
    ClientError,
    ServerError,
    NotFound404Error,
)
from .serializers import SimpleSerializer
from .tapioca import TapiocaInstantiator


def generate_wrapper_from_adapter(adapter_class):
    return TapiocaInstantiator(adapter_class)


class TapiocaAdapter(object):
    serializer_class = SimpleSerializer

    def __init__(self, serializer_class=None, *args, **kwargs):
        if serializer_class:
            self.serializer = serializer_class()
        else:
            self.serializer = self.get_serializer()

    def _get_to_native_method(self, method_name, value):
        if not self.serializer:
            raise NotImplementedError("This client does not have a serializer")

        def to_native_wrapper(**kwargs):
            return self._value_to_native(method_name, value, **kwargs)

        return to_native_wrapper

    def _value_to_native(self, method_name, value, **kwargs):
        return self.serializer.deserialize(method_name, value, **kwargs)

    def get_serializer(self):
        if self.serializer_class:
            return self.serializer_class()

    def get_api_root(self, api_params):
        return self.api_root

    def fill_resource_template_url(self, template, params):
        return template.format(**params)

    def get_request_kwargs(self, api_params, *args, **kwargs):
        """Обогащение запроса, параметрами"""
        serialized = self.serialize_data(kwargs.get("data"))

        kwargs.update({"data": self.format_data_to_request(serialized)})
        return kwargs

    def generate_request_kwargs(self, api_params, *args, **kwargs):
        """
        При необходимости,
        здесь можно создать несколько наборов параметров для того,
        чтобы сделать несколько запросов
        """
        return [self.get_request_kwargs(api_params, *args, **kwargs)]

    def get_error_message(self, data, response=None):
        return str(data)

    def process_response(self, response, **request_kwargs):
        if response.status_code == 404:
            raise ResponseProcessException(NotFound404Error, None)
        elif 500 <= response.status_code < 600:
            raise ResponseProcessException(ServerError, None)

        data = self.response_to_native(response)

        if 400 <= response.status_code < 500:
            raise ResponseProcessException(ClientError, data)

        return data

    def serialize_data(self, data):
        if self.serializer:
            return self.serializer.serialize(data)

        return data

    def format_data_to_request(self, data):
        raise NotImplementedError()

    def response_to_native(self, response):
        raise NotImplementedError()

    def get_iterator_list(self, response_data):
        raise NotImplementedError()

    def get_iterator_next_request_kwargs(
        self, iterator_request_kwargs, response_data, response
    ):
        raise NotImplementedError()

    def is_authentication_expired(self, exception, *args, **kwargs):
        return False

    def refresh_authentication(self, api_params, *args, **kwargs):
        raise NotImplementedError()

    def retry_request(
        self,
        response,
        tapioca_exception,
        api_params,
        count_request_error,
        *args,
        **kwargs
    ):
        """
        Условия повторения запроса.

        Некоторые доступные данные:
        response = tapioca_exception.client().response
        status_code = tapioca_exception.client().status_code
        response_data = tapioca_exception.client().data
        """
        return False

    def wrapper_call_exception(
        self, response, tapioca_exception, api_params, *args, **kwargs
    ):
        """
        Для вызова кастомных исключений.
        Когда например сервер отвечает 200,
        а ошибки передаются в внутри json.
        """
        raise tapioca_exception

    def extra_request(
        self, api_params, current_request_kwargs, request_kwargs_list, response, current_result
    ):
        """
        Дополнительные запросы.
        Будут сделаны, если отсюда вернется список kwargs доп. запросов
        Если вернется пустой request_kwargs_list, то не будут сделаны.

        :param current_request_kwargs: dict : {headers, data, url, params} : параметры текущего запроса
        :param request_kwargs_list: list : параметры запросов, которые предстоит сделать
        :param response: request object : текущий ответ
        :param current_result: json : текущий результат
        :return: list : request_kwargs_list
        """
        return request_kwargs_list

    def __str__(self, data, request_kwargs, response, api_params):
        raise NotImplementedError()


class FormAdapterMixin(object):
    def format_data_to_request(self, data):
        return data

    def response_to_native(self, response):
        return {"text": response.text}


class JSONAdapterMixin(object):
    def get_request_kwargs(self, api_params, *args, **kwargs):
        arguments = super(JSONAdapterMixin, self).get_request_kwargs(
            api_params, *args, **kwargs
        )

        if "headers" not in arguments:
            arguments["headers"] = {}
        arguments["headers"]["Content-Type"] = "application/json"
        return arguments

    def format_data_to_request(self, data):
        if data:
            return json.dumps(data)

    def response_to_native(self, response):
        if response.content.strip():
            return response.json()

    def get_error_message(self, data, response=None):
        if not data and response.content.strip():
            data = json.loads(response.content.decode("utf-8"))

        if data:
            return data.get("error", None)

    def transform_results(self, results, request_kwargs, response, api_params):
        """Преобразует данные после получения всех ответов"""
        return results

    def data(self, data, request_kwargs, response, api_params, *args, **kwargs):
        """Преобразует данные в json"""
        return data

    def json(self, data, request_kwargs, response, api_params, *args, **kwargs):
        """Преобразует данные в json"""
        return data

    def to_df(self, data, request_kwargs, response, api_params, *args, **kwargs):
        """Преобразование в DataFrame"""
        raise NotImplementedError()

    def transform(self, data, request_kwargs, response, api_params, *args, **kwargs):
        """Кастомное преобразование данных"""
        raise NotImplementedError()


class XMLAdapterMixin(object):
    def _input_branches_to_xml_bytestring(self, data):
        if isinstance(data, Mapping):
            return xmltodict.unparse(data, **self._xmltodict_unparse_kwargs).encode(
                "utf-8"
            )
        try:
            return data.encode("utf-8")
        except Exception as e:
            raise type(e)(
                "Format not recognized, please enter an XML as string or a dictionary"
                "in xmltodict spec: \n%s" % e.message
            )

    def get_request_kwargs(self, api_params, *args, **kwargs):
        # stores kwargs prefixed with 'xmltodict_unparse__' for use by xmltodict.unparse
        self._xmltodict_unparse_kwargs = {
            k[len("xmltodict_unparse__"):]: kwargs.pop(k)
            for k in kwargs.copy().keys()
            if k.startswith("xmltodict_unparse__")
        }
        # stores kwargs prefixed with 'xmltodict_parse__' for use by xmltodict.parse
        self._xmltodict_parse_kwargs = {
            k[len("xmltodict_parse__"):]: kwargs.pop(k)
            for k in kwargs.copy().keys()
            if k.startswith("xmltodict_parse__")
        }

        arguments = super(XMLAdapterMixin, self).get_request_kwargs(
            api_params, *args, **kwargs
        )

        if "headers" not in arguments:
            arguments["headers"] = {}
        arguments["headers"]["Content-Type"] = "application/xml"
        return arguments

    def format_data_to_request(self, data):
        if data:
            return self._input_branches_to_xml_bytestring(data)

    def response_to_native(self, response):
        if response.content.strip():
            if "xml" in response.headers["content-type"]:
                return xmltodict.parse(response.content, **self._xmltodict_parse_kwargs)
            return {"text": response.text}
