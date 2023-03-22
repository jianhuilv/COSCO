
from .Workload import *
class DFW(Workload):
    def __init__(self,env):
        super.__init__()
        self.containerList = []
        self.env = env

    def generateNewContainersProcess(self):
        """
        一个离散进程，可以用于生成新的任务负载
        """

        while not self.stop:
            message = yield self.env.workload