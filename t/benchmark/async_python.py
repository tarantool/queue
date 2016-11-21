import psutil

from hurry.filesize import size, alternative

import gevent
import gtarantool

def worker(tnt, timeout):
    while True:
        try:
            task = tnt.call("queue.tube.mail_msg:take", (timeout, ))
            if task:
                tnt.call("queue.tube.mail_msg:ack", (task[0][0], ))
        except Exception as e:
            print "Worker connection is closed", e
            return

def spawn_workers(workers, timeout, connection):
    jobs = []
    for _ in range(workers):
        jobs.append(gevent.spawn(worker, connection, timeout))
    return

def find_pid_benchmark():
    for proc in psutil.process_iter():
        if proc.name().find('tarantool') >= 0:
            return proc
    return None

def spawn_printer():
    proc = find_pid_benchmark()
    if proc == None:
        raise Exception("Can't find benchmark process")
    tnt = gtarantool.connect("127.0.0.1", 3301)
    while True:
        try:
            stat = tnt.call("box.stat")[0][0]
        except:
            print "Printer connection is closed"
            return
        print "%6.2f CPU | %6s MEM [ DELETE %6d rps | INSERT %6d rps | SELECT %6d rps ]" % (
                proc.cpu_percent(), size(proc.memory_info().rss, system=alternative),
                stat['DELETE']['rps'],
                stat['INSERT']['rps'],
                stat['SELECT']['rps']
        )
        gevent.sleep(1)

def main():
    workers = 200
    timeout = 2.5
    wcon = gtarantool.connect("127.0.0.1", 3301)
    spawn_workers(workers, timeout, wcon)
    spawn_printer()
    return 0

if __name__ == "__main__":
    exit(main())
