import sys
from time import time
from mpi4py import MPI


# mpiexec  -n 5 python

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

TIMEOUT = 0.2
REQUEST_TAG = 000
ELECTION_TAG = 001
LEADERSHIP_TAG = 010
ACCEPT_LEADERSHIP_TAG = 011
ELECTION_ANSWER_TAG = 100
COORDINATE_ANSWER_TAG = 101

leader = size - 1
request = "Are you there?"
is_candidate = False
action = int(sys.argv[1])


def send_to_higher(text, tag, process_id):
    for x in range(size):
        if process_id < x:
            comm.isend(text, dest=x, tag=tag)


def receive(source=None, tag=111):
    if source is None:
        source = MPI.ANY_SOURCE

    r = comm.irecv(source=source, tag=tag)

    start = time()

    while True:
        answer = r.test()

        if time() - start > TIMEOUT or answer[0]:
            break

    return answer


# leader receive 'are you there?'
def checking_coordinator_message():
    result = receive(tag=REQUEST_TAG)
    print(rank, "checking_coordinator_message", result)
    sys.stdout.flush()

    if result[0]:
        if result[1] == request and action == 0:
            send_to_higher("Yes, I am!", COORDINATE_ANSWER_TAG, -1)
            pass


# receive answer from leader
def checking_coordinator_answer():
    result = receive(leader, tag=COORDINATE_ANSWER_TAG)
    print(rank, "checking_coordinator_answer", result)
    sys.stdout.flush()
    return result[0]


# send election message
def election_message():

    election_data = {
        "txt": "Election",
        "process": rank
    }

    send_to_higher(election_data, ELECTION_TAG, rank)
    print(election_data, "send election!")
    sys.stdout.flush()


# receive election message
def election_answer():
    result = receive(tag=ELECTION_TAG)
    print(rank, 'receive election!', result)
    sys.stdout.flush()

    if result[0]:
        send_ok_answer(result[1])
        return True

    return False


def receive_in_loop(tag):
    maybe_alives = [comm.irecv(source=x, tag=tag) for x in range(size)]
    alives = []

    start = time()

    while True:

        for i, req in enumerate(maybe_alives):
            answer = req.test()

            if answer[0]:
                alives.append(answer[1]['process'])
                maybe_alives.pop(i)

        if time() - start > TIMEOUT or len(maybe_alives) == 0:
            break

    return alives


def receive_ok(tag):
    result = receive_in_loop(tag)

    #receive any "ok"
    if len(result):
        return False

    return True


def send_ok_answer(candidates):
    print(rank, 'send election answer (OK!)')
    sys.stdout.flush()
    accept_data = {
        "txt": "OK",
        "process": rank
    }
    [comm.isend(accept_data, dest=x, tag=ELECTION_ANSWER_TAG) for x in candidates]


# receive ok answer
def receive_election_answer():
    result = receive(tag=ELECTION_ANSWER_TAG)
    print(rank, 'receive election answer (OK!)', result)
    sys.stdout.flush()

    if not result[0] and is_candidate:
        declare_the_leader()


def declare_the_leader():
    print(rank, "I am a leader!")
    sys.stdout.flush()
    send_to_higher(rank, ACCEPT_LEADERSHIP_TAG, -1)
    return rank


def accept_leadership():
    result = receive(tag=ACCEPT_LEADERSHIP_TAG)
    print(rank, 'accept_leadership', result)
    sys.stdout.flush()

    return result[1]


if __name__ == "__main__":
    if rank == leader:
        checking_coordinator_message()          #Yes I am!

    else:
        comm.isend(request, dest=leader, tag=REQUEST_TAG)

        if not checking_coordinator_answer():
            # bully algorithm
            election_message()
            candidates = receive_in_loop(ELECTION_TAG)
            send_ok_answer(candidates)
            is_leader = receive_ok(ELECTION_ANSWER_TAG)
            print(rank, "is leader?", is_leader)
            sys.stdout.flush()
            if is_leader:
                declare_the_leader()
            leader = accept_leadership()

    print('leader', leader)
    sys.stdout.flush()
    print('end')
