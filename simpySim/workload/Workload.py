class Workload:
    def __init__(self):
        self.containerList = []
        self.stop = False
        self.env = None

    def setEnv(self, env):
        self.env = env

    def getUnDeployedContainers(self):
        undeployed = []
        for i, deployed in enumerate(self.deployedContainers):
            if not deployed:
                undeployed.append(self.createdContainers[i])
        return undeployed

    def updateDeployedContainers(self, creationIDs):
        for cid in creationIDs:
            assert not self.deployedContainers[cid]
            self.deployedContainers[cid] = True
