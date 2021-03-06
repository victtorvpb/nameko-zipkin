from nameko_zipkin.transport import HttpTransport
from nameko_zipkin import monkey_patch
from py_zipkin import zipkin


def decorator_http_transport(url, span_name, debug=True):
    """
    [summary]
    Decorator to implement the zipkin nameko in service RPC
    :param url: Url to Zipkin Service
    :type url: String
    :param span_name: Span Name
    :type span_name: String
    :param span_name: Flag to execute or no the zipkin
    :type span_name: Debug
    :return:
    :rtype: None
    """

    def decorator_zipkin(func):
        def wrapper(*args, **kwargs):
            try:
                service_name = args[0].name
            except AttributeError:
                service_name = "unknown"

            if not debug:
                return func(*args, **kwargs) 
            if hasattr(args[0], "zipkin_nameko") and not args[0].zipkin_nameko:
                handler = HttpTransport(url).handle
                monkey_patch(handler)
                with zipkin.zipkin_server_span(
                    service_name,
                    span_name,
                    sample_rate=100.0,
                    transport_handler=handler,
                ):
                    return func(*args, **kwargs)
            else:
                return func(*args, **kwargs)

        return wrapper

    return decorator_zipkin
