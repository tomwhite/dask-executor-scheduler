# Pywren "Hello World" to see if Pywren is working

import pywren_ibm_cloud as pywren

def hello(name):
    return 'Hello {}!'.format(name)

pw = pywren.local_executor()
#pw = pywren.cloudrun_executor()
#pw = pywren.cloudrun_executor(log_level="DEBUG")
pw.call_async(hello, 'World')
print(pw.get_result())
