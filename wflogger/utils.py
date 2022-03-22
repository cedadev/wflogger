import re


nodename_regex = re.compile(r"nodename=(?P<nodes>.+?)\s+.*features=(?P<spec>.+?)\s+")

def parse_slurm_config(conf_path="/etc/slurm/slurm.conf"):
    with open(conf_path) as conf:
        for line in conf:
            line = line.lower().strip()
            match = nodename_regex.match(line)
            if match:
                print(match.groupdict())

    

parse_slurm_config()
