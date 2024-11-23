import psutil
import subprocess

def check_python_process(filename):
    command = f"pgrep -f '{filename}'"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, _ = process.communicate()
    return len(out.strip()) > 0


def count_processes(name, file):
    # for q in psutil.process_iter():
    #     if name in q.name().lower() and file in q.cmdline() and q.status() == 'sleeping' :
    #         p = psutil.Process(q.pid)
    #         p.terminate()
            
    return sum(name in q.name().lower() and file in q.cmdline() for q in psutil.process_iter())

def check_if_running(filename):
    if count_processes('python', filename) > 3:
        return False
        
    else:
        return True
    
def check_current_status(filename):
    if check_python_process(filename):
        return True
    else:
        return False