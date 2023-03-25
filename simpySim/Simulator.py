import simpy
from time import time, sleep
from .util import ColorUtils
from framework.workload import DeFogWorkload


def visualSleep(t):
    total = str(int(t // 60)) + " min, " + str(t % 60) + " sec"
    for i in range(int(t)):
        print("\r>> Interval timer " + str(i // 60) + " min, " + str(i % 60) + " sec of " + total, end=' ')
        sleep(1)
    sleep(t % 1)
    print()


class Sim:

    def __init__(self, env, datacenter, scheduler, workload, isCoSimulate):
        self.simpyEnv = simpy.Environment()

        self.env = env

        self.until = 0

        self.__idProcess = -1

        self.stop = False

        self.datacenter = datacenter

        self.scheduler = scheduler

        self.workload = workload

        self.workload_pipe = simpy.Store(self.simpyEnv)

        self.decision_pipe = simpy.Store(self.simpyEnv)

        # 存储正在运行的所有容器的list，根据cid可以获得指定的容器

    def generateNewContainersProcess(self):
        """
        一个离散进程，可以用于生成新的任务负载
        """

        while not self.stop:
            # 生成新任务，进入等待队列
            NewContainerInfo = self.workload.generateNewContainers()
            self.workload_pipe.put(NewContainerInfo)
            # todo 另外这个n应该是个参数，可以传进来的
            yield self.simpyEnv.timeout(0)
            # 暂停三十秒后，再次生成任务

    def ctrlProcessMonitor(self):
        # 原生的simpy.run.until 函数会检查管道，直到所有管道为空，
        while True:
            yield self.simpyEnv.timeout(0)
            if self.until:
                if self.simpyEnv.now >= self.until:
                    self.stop = True

    def schedulerProcess(self):
        """
        一个离散进程，根据当前的containerList，以及从workload_pipe中获取的container
        生成decision
        """
        while not self.stop:
            newJobInfo = yield self.workload_pipe.get()
            deployed, destroyed = self.env.addContainers(newJobInfo)  # Deploy new containers and get container IDs
            # todo 把已经完成的，迁移出containerList
            start = time()
            selected = self.scheduler.selection()  # Select container IDs for migration
            schedulingTime = time() - start
            decisions = self.scheduler.filter_placement(
                self.scheduler.placement(selected + deployed))
            for decision in decisions:
                self.decision_pipe.put(decision)

            ColorUtils.printDecision(decisions)

            yield self.simpyEnv.timeout(schedulingTime)

    def migrateProcess(self):
        """
        一个离散进程，用于容器的前移，执行调度的任务
        """
        while not self.stop:
            decision = yield self.decision_pipe.get()
            cid, hid = decision
            container = self.env.containerList[cid]
            currentHostID = container.getHostID()
            currentHost = self.env.getHostByID(currentHostID)
            targetHost = self.env.getHostByID(hid)
            if hid == currentHostID or self.getPlacementPossible(cid, hid) is False:
                yield self.simpyEnv.timeout(0)  # 不允许迁移，直接返回
            if self.env.containerList[cid].hostid == -1:
                yield self.simpyEnv.timeout(container.simpyAllocateAndExecute())
            else:
                yield self.simpyEnv.timeout(container.simpyAllocateAndRestore())

    def monitorProcess(self):
        """
        一个离散进程，用于每30秒监控一次容器的分配
        :return:
        """

    def run(self, until):
        """
        Start the Simulator
        :param until: the simulate times
        :return: null
        """
        # 启动工作负载
        self.simpyEnv.process(self.generateNewContainersProcess())

        # 启动调度负载
        self.simpyEnv.process(self.schedulerProcess())

        # 启动迁移负载
        self.simpyEnv.process(self.migrateProcess())

        self.simpyEnv.process(self.ctrlProcessMonitor())

        self.simpyEnv.run(self.until)

    # 检查磁盘空间是否足够
    def getPlacementPossible(self, cid, hid):
        container = self.env.containerList[cid]
        host = self.env.hostList[hid]
        ipsReq = container.getBaseIPS()
        ramSizeReq, ramReadReq, ramWriteReq = container.getRAM()
        diskSizeReq, diskReadReq, diskWriteReq = container.getDisk()
        ipsAvailable = host.getIPSAvailable()
        ramSizeAv, ramReadAv, ramWriteAv = host.getRAMAvailable()
        diskSizeAv, diskReadAv, diskWriteAv = host.getDiskAvailable()
        return (ipsReq <= ipsAvailable and
                ramSizeReq <= ramSizeAv and
                diskSizeReq <= diskSizeAv
                )
