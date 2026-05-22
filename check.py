import re

K = 2
in_cs = set()
max_in_cs = 0
weeding_events = 0

with open("log.txt", "r") as f:
    for line in f:
        line = line.strip()
        m_enter = re.search(r"\[(\d+)\] \[\d+\] Jestem w sekcji krytycznej", line)
        if m_enter:
            pid = int(m_enter.group(1))
            in_cs.add(pid)
            if len(in_cs) > max_in_cs:
                max_in_cs = len(in_cs)
            if len(in_cs) > K:
                print(f"ERROR: K={K}, but currently in CS: {in_cs}")
            continue

        m_leave = re.search(r"\[P(\d+)\] \[\d+\] WEEDING -> REST", line)
        if m_leave:
            pid = int(m_leave.group(1))
            if pid in in_cs:
                in_cs.remove(pid)
                weeding_events += 1
            else:
                print(f"ERROR: Process {pid} left CS but wasn't in it!")

print(f"Max in CS: {max_in_cs}")
print(f"Total weeding events: {weeding_events}")
