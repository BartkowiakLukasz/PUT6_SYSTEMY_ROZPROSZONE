import subprocess
import re
import sys

# Parameters
np = 6
G = 2
L = 10
D = 10
iterations = 3
cs_ms = 100
shopping_ms = 100

K = min(G, L)

cmd = f"wsl mpirun -np {np} ./gardener {G} {L} {D} {iterations} {shopping_ms} {cs_ms}"
proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

in_cs = set()
max_in_cs = 0
weeding_events = 0

pattern_enter = re.compile(r"\[(\d+)\] \[\d+\] Jestem w sekcji krytycznej")
pattern_leave = re.compile(r"\[P(\d+)\] \[\d+\] WEEDING -> REST")

history = []

for line in proc.stdout:
    line = line.strip()
    if not line:
        continue
    
    history.append(line)
    if len(history) > 200:
        history.pop(0)

    m_enter = pattern_enter.search(line)
    if m_enter:
        pid = int(m_enter.group(1))
        in_cs.add(pid)
        if len(in_cs) > max_in_cs:
            max_in_cs = len(in_cs)
        if len(in_cs) > K:
            print("\n".join(history))
            print(f"ERROR: Too many in CS at once! K={K}, but currently in CS: {in_cs}")
            proc.terminate()
            sys.exit(1)
        continue

    m_leave = pattern_leave.search(line)
    if m_leave:
        pid = int(m_leave.group(1))
        if pid in in_cs:
            in_cs.remove(pid)
            weeding_events += 1
        else:
            print(f"ERROR: Process {pid} left CS but wasn't in it!")
            proc.terminate()
            sys.exit(1)

proc.wait()
