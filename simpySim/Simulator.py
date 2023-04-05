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

    def __init__(self, env, datacenter, scheduler, workload, isCoSimulate, steps):
        self.simpyEnv = simpy.Environment()

        self.env = env

        self.isCoSimulate = isCoSimulate

        self.__idProcess = -1

        self.stop = False

        self.steps = steps

        self.datacenter = datacenter

        self.scheduler = scheduler

        self.workload = workload

        self.schedule_flag = simpy.Store(self.simpyEnv)

        self.migrate_flag = simpy.Store(self.simpyEnv)

        self.execute_flag = simpy.Store(self.simpyEnv)

        self.workload_pipe = simpy.Store(self.simpyEnv)

        self.decision_pipe = simpy.Store(self.simpyEnv)

        self.execute_pipe = simpy.Store(self.simpyEnv)
        # 存储正在运行的所有容器的list，根据cid可以获得指定的容器

    def generateNewContainersProcessForSimulate(self):
        """
        一个离散进程，可以用于生成新的任务负载
        """
        interval = 0
        while interval < self.steps:
            print("generating")
            # 生成新任务，进入等待队列
            interval += 1
            NewContainerInfo = self.workload.generateNewContainers(interval)
            data = (interval, NewContainerInfo)
            self.workload_pipe.put(data)

            print("round: " + str(interval))
            yield self.schedule_flag.get()
            yield self.simpyEnv.timeout(0)
            # todo
            #  这里应该加一个更新状态的，但是要保证这个时候所有的分配任务都已经完成。
            #  用pipe或者其他变量，去接收？

    def generateNewContainersProcessForCo_simulate(self):
        """
        一个离散进程，可以用于生成新的任务负载
        """
        interval = 0
        while interval < self.steps:
            print("generating")
            # 生成新任务，进入等待队列
            interval += 1
            NewContainerInfo = self.workload.generateNewContainers(interval)
            data = (interval, NewContainerInfo)
            self.workload_pipe.put(data)

            print("round: " + str(interval))
            visualSleep(30)
            yield self.schedule_flag.get()
            yield self.simpyEnv.timeout(0)
            # todo
            #  这里必须等所有任务更新三十秒后，再次生成任务，或者单个去更新workload

    def schedulerProcess(self):
        """
        一个离散进程，根据当前的containerList，以及从workload_pipe中获取的container
        生成decision
        """
        while True:
            data = yield self.workload_pipe.get()
            print("scheduling")
            interval, newJobInfo = data
            deployed, destroyed = self.env.addContainers(newJobInfo)  # Deploy new containers and get container IDs

            start = time()
            selected = self.scheduler.selection()  # Select container IDs for migration
            schedulingTime = time() - start
            decisions = self.scheduler.filter_placement(
                self.scheduler.placement(selected + deployed))

            ColorUtils.printDecision(decisions)

            migrations = []
            for i, (cid, hid) in enumerate(decisions):
                container = self.env.getContainerByID(cid)
                currentHostID = self.env.getContainerByID(cid).getHostID()
                currentHost = self.env.getHostByID(currentHostID)
                targetHost = self.env.getHostByID(hid)
                if hid != self.env.containerlist[cid].hostid and self.getPlacementPossible(cid, hid):
                    migrations.append((cid, hid))

            for i, (cid, hid) in enumerate(migrations):
                statusCode = 0
                if i == len(migrations) - 1:
                    statusCode = 1
                data = (statusCode, cid, hid)
                self.decision_pipe.put(data)

            # 等待migrate运行完成
            yield self.migrate_flag.get()

            for (cid, hid) in decisions:
                if self.env.containerlist[cid].hostid == -1:
                    self.env.containerlist[cid] = None

            self.workload.updateDeployedContainers(
                self.env.getCreationIDs(migrations, deployed))

            self.schedule_flag.put(True)
            yield self.simpyEnv.timeout(0)

    def schedulerProcessForSimulate(self):
        """
        一个离散进程，根据当前的containerList，以及从workload_pipe中获取的container
        生成decision
        运行在管理者层
        """
        while True:
            newJobInfo = yield self.workload_pipe.get()
            print("scheduling")
            interval, newJobInfo = newJobInfo
            deployed, destroyed = self.env.addContainers(newJobInfo)  # Deploy new containers and get container IDs
            needToWaitExecute = False
            start = time()
            selected = self.scheduler.selection()  # Select container IDs for migration
            schedulingTime = time() - start
            decisions = self.scheduler.filter_placement(
                self.scheduler.placement(selected + deployed))
            containerIDsAllocated = []

            ColorUtils.printDecision(decisions)

            migrations = []
            for i, (cid, hid) in enumerate(decisions):
                container = self.env.getContainerByID(cid)
                currentHostID = self.env.getContainerByID(cid).getHostID()
                currentHost = self.env.getHostByID(currentHostID)
                targetHost = self.env.getHostByID(hid)
                if hid != self.env.containerlist[cid].hostid and self.getPlacementPossible(cid, hid):
                    migrations.append((cid, hid))
                    containerIDsAllocated.append(cid)

            for i, (cid, hid) in enumerate(migrations):
                statusCode = 0
                if i == len(migrations) - 1:
                    statusCode = 1
                data = (statusCode, cid, hid)
                self.decision_pipe.put(data)

            # 等待migrate运行完成
            yield self.migrate_flag.get()

            for (cid, hid) in decisions:
                if self.env.containerlist[cid].hostid == -1:
                    self.env.containerlist[cid] = None

            for i, container in enumerate(self.env.containerlist):
                if container and i not in containerIDsAllocated:
                    print("executing: " + str(container.id) + ", " + str(container.getHostID()))
                    container.execute(0)

            # 更新获取运行状态
            self.workload.updateDeployedContainers(
                self.env.getCreationIDs(migrations, deployed))

            self.schedule_flag.put(True)
            yield self.simpyEnv.timeout(0)

    def migrateProcessForCo_simulate(self):
        """
        一个离散进程，用于容器的迁移，执行调度的任务，用于耦合数据
        """
        while True:
            data = yield self.decision_pipe.get()
            statusCode, cid, hid = data
            decision = (cid, hid)
            print("migrating: " + str(decision))
            cid, hid = decision
            container = self.env.containerlist[cid]
            if container is None:
                yield self.simpyEnv.timeout(0)
            currentHostID = container.getHostID()
            currentHost = self.env.getHostByID(currentHostID)
            targetHost = self.env.getHostByID(hid)
            if hid == currentHostID or self.getPlacementPossible(cid, hid) is False:
                yield self.simpyEnv.timeout(0)  # 不允许迁移，直接返回
            if self.env.containerlist[cid].hostid == -1:
                container.simpyAllocateAndExecute()
            else:
                container.simpyAllocateAndRestore()
            if statusCode == 1:
                self.migrate_flag.put(True)

            yield self.simpyEnv.timeout(0)

    def migrateProcessForSimulate(self):
        """
        一个离散进程，用于容器的迁移，用于虚拟仿真，不需要执行迁移
        该项目运行在agent层，主要用于做迁移工作。
        """
        while True:

            data = yield self.decision_pipe.get()
            statusCode, cid, hid = data
            decision = (cid, hid)
            print("migrating: " + str(decision))
            # print(decision)
            routerBwToEach = self.env.totalbw / len(decision) if len(decision) > 0 else self.env.totalbw
            migrations = []
            containerIDsAllocated = []
            cid, hid = decision
            container = self.env.containerlist[cid]
            currentHostID = container.getHostID()
            currentHost = self.env.getHostByID(currentHostID)
            targetHost = self.env.getHostByID(hid)
            decisionList = [decision]
            migrateFromNum = len(self.scheduler.getMigrationFromHost(currentHostID, decisionList))
            migrateToNum = len(self.scheduler.getMigrationToHost(hid, decisionList))
            allocBw = min(targetHost.bwCap.downlink / migrateToNum, currentHost.bwCap.uplink / migrateFromNum,
                          routerBwToEach)
            if hid != self.env.containerlist[cid].hostid and self.getPlacementPossible(cid, hid):
                migrations.append((cid, hid))
                containerIDsAllocated.append(cid)
                migrateAndExecuteTime = container.allocateAndExecute(hid, allocBw)
                # todo
                #  这里可能有误会，只需要存储migrateAndExecuteTime
                #  不需要真的去yield
            if statusCode == 1:
                self.migrate_flag.put(True)

            yield self.simpyEnv.timeout(0)

    def monitorProcess(self):
        """，原先是在每个单元时间结束后，检测所有的运行信息，会统计哪些信息呢？
        现在需要更改监控的
        疑难点方式。
        :return:
        """
        pass

    def run(self):
        """
        Start the Simulator
        :param until: the simulate times
        :return: null
        """
        # 启动工作负载

        # 启动调度负载

        # 启动迁移负载
        if self.isCoSimulate:
            self.simpyEnv.process(self.generateNewContainersProcessForCo_simulate())
            self.simpyEnv.process(self.schedulerProcess())
            self.simpyEnv.process(self.migrateProcessForCo_simulate())
        else:
            self.simpyEnv.process(self.generateNewContainersProcessForSimulate())
            self.simpyEnv.process(self.schedulerProcessForSimulate())
            self.simpyEnv.process(self.migrateProcessForSimulate())

        self.simpyEnv.run()

    # 检查磁盘空间是否足够
    def getPlacementPossible(self, cid, hid):
        container = self.env.containerlist[cid]
        host = self.env.hostlist[hid]
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
