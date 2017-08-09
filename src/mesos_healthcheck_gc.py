# coding=utf-8

import psutil
import re
import os
import signal
import docker


__all__ = ["MesosHealthCheckCleaner", ]


class APIClient(object):
    def __init__(self, socket="unix://var/run/docker.sock"):
        self.client = docker.Client(base_url=socket)

    def containers(self):
        # hack here, get mesos task containers ONLY.
        return filter(lambda c: "mesos" in c["Names"][0],
                      self.client.containers())


class MesosTaskNameNotFound(Exception):
    pass


class MesosHealthCheckIterator(object):
    pattern = re.compile(
        r'/usr/libexec/mesos/mesos-health-check.*docker\ exec\ (?P<MesosTaskName>mesos[\-0-9a-zA-Z\.]+)\ sh\ -c'
    )

    @classmethod
    def mesos_task_name(cls, full_name):
        match = cls.pattern.search(full_name)
        if match:
            return match.group("MesosTaskName")
        else:
            raise MesosTaskNameNotFound

    @classmethod
    def iterate(cls):
        for proc in psutil.process_iter():
            try:
                pinfo = proc.as_dict(attrs=['pid', 'cmdline'])
                pinfo["mesos_task_name"] = cls.mesos_task_name(
                    " ".join(pinfo["cmdline"]),
                )
            except (psutil.NoSuchProcess, MesosTaskNameNotFound) as e:
                pass
            else:
                yield pinfo


class Cleaner(object):
    def __init__(self):
        pass


class MesosHealthCheckCleaner(Cleaner):
    def __init__(self):
        self.cli = APIClient()
        super(MesosHealthCheckCleaner, self).__init__()

    def clean(self):
        # collect the mesos-health-check processes with `mesos-xxx` name.
        # list the alive mesos tasks via docker client.
        # then, clean up the out-of-date processes.
        mesos_containers = self.cli.containers()
        wait_to_cleans = []

        for mesos_health_check in MesosHealthCheckIterator.iterate():
            if not filter(lambda mc:  "/" + mesos_health_check["mesos_task_name"] == mc["Names"][0],
                          mesos_containers):
                wait_to_cleans.append(mesos_health_check)

        print "[MesosHealthCheckCleaner] Need to kill {} processes...".format(
            len(wait_to_cleans),
        )

        # bulk kill the health-check
        for w2kill in wait_to_cleans:
            print "Killing mesos health check process: {}, name: {}".format(
                w2kill["pid"], w2kill["mesos_task_name"]
            )
            os.kill(w2kill["pid"], signal.SIGTERM)


def main():
    MesosHealthCheckCleaner().clean()


if __name__ == "__main__":
    main()
