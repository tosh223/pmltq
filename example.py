from queuing_hub.publisher import Publisher
from queuing_hub.subscriber import Subscriber
from queuing_hub.forwarder import Forwarder


def main():
    pub = Publisher()
    print(pub.topic_list)
    print(pub.push(pub.topic_list, 'hey topics!'))

    sub = Subscriber()
    print(sub.sub_list)
    print(sub.qsize())
    print(sub.pull(sub.sub_list, 1))

    fwd = Forwarder(sub=sub.sub_list[2], topic=pub.topic_list[0], max_num=1)
    print(fwd.pass_through())
    print(fwd.transport())


if __name__ == "__main__":
    main()
