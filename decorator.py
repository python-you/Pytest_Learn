import logging


def use_logging(level):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if level == 'warn':
                logging.warn("%s is running" % func.__name__)
            elif level == "info":
                logging.info("%s is running" % func.__name__)
            return func(*args)

        return wrapper

    return decorator


@use_logging(level="info")
def foo(name = "smart"):
    print("i am %s" % name)


foo()
