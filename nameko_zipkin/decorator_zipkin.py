from nameko_zipkin.transport import HttpTransport
from nameko_zipkin import monkey_patch
from py_zipkin import zipkin


def decorator_http_transport(url, service_name, span_name):
    def decorator_zipkin(func):
        def wrap(*original_args, **original_kwargs):
            if not original_args[0].zipkin_nameko:
                handler = HttpTransport(url).handle
                monkey_patch(handler)
                with zipkin.zipkin_server_span(service_name,
                                            span_name,
                                            sample_rate=100.,
                                            transport_handler=handler):
                    func(*original_args, **original_kwargs)
            else:
                func(*original_args, **original_kwargs)
        return wrap

    return decorator_zipkin
