import logging
 
def logger():
    """
    A decorator that wraps the passed in function and logs 
    exceptions should one occur
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            funcName = func.__name__
            try:
            	logging.warn('********************************* start ' + funcName + ' *********************************')  
                return func(*args, **kwargs)
            except:
                # log the exception
                err = "There was an exception in  "
                err += funcName
                logging.exception(err)
            finally:
                logging.warn('********************************* end ' + funcName + ' *********************************')  
            # re-raise the exception
            raise
        return wrapper
    return decorator

@logger()
def zero_divide():
	1 / 0
	print("zero divide, this should never print")

@logger()
def normal_divide():
	10 / 1
	print("normal divide, this should always print")


if __name__ == '__main__':
	#this function should complete and log the start/end
	normal_divide()
	#this function should error, log the error and break the script
	zero_divide()
	