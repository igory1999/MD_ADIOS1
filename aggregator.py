import adios2
import numpy as np
import subprocess
import logging
from myutils import *
import time
import random
import sys
import glob
import os

def q_kill_simulation(data):
    metrics = data.sum()/(data.shape[0]*data.shape[1])
    return metrics > 0.7

if(__name__ == '__main__'):
    current_dir = sys.argv[1]
    run_dir = sys.argv[2]
    print(f"In aggregator: {current_dir}")
    dir_aggregator = f'{run_dir}/aggregator'
    dir_simulations = f'{run_dir}/simulations'
    ADIOS_XML = f'{current_dir}/adios.xml'

    
    logging.basicConfig(filename=f'{dir_aggregator}/aggregator.log', filemode='w', level=logging.INFO)
    logging.info("Start")
    logging.info(get_now())

    sim_streams = {}
    sim_data = {}
    
    max_iterations = 300
    
    while(max_iterations > 0):
        new_simulations = glob.glob(f"{dir_simulations}/new/*")
        print(f"New simulations at {max_iterations}: {new_simulations}")
        sys.stdout.flush()
        for n in new_simulations:
            r = n.replace("new","running")
            a = n.replace("new","all")
            subprocess.getstatusoutput(f"mv {n} {r}")
            print(f"{a}/SimulationOutput.bp")
            print(ADIOS_XML)
            sys.stdout.flush()
            simulation_stream = adios2.open(name=f"{a}/SimulationOutput.bp",
                                            mode="r", config_file=ADIOS_XML,
                                            io_in_config_file="SimulationOutput")
            rb = os.path.basename(r)
            sim_streams[rb] = simulation_stream
            
        remove = []
        for sim_dir in sim_streams.keys():
            fstep = next(sim_streams[sim_dir])
            step = fstep.current_step()
            logging.info("="*30)
            logging.info(f"sim_dir = {sim_dir}, step = {step}")
            data = fstep.read("MyData")
            logging.info(f"data = {data}")
            try:
                sim_data[sim_dir].append(data)
            except:
                sim_data[sim_dir] = [data]
            if(q_kill_simulation(data)):
                logging.info(f"Killing the simulation in {sim_dir}")
                subprocess.Popen(["touch",f"{dir_simulations}/all/{sim_dir}/stop.simulation"])
                sim_streams[sim_dir].close()
                remove.append(sim_dir)
                subprocess.getstatusoutput(f"mv {dir_simulations}/running/{sim_dir} {dir_simulations}/stopped/{sim_dir}") 
        for r in remove:
            del sim_streams[r]
        max_iterations -= 1

    subprocess.getstatusoutput(f"touch {dir_aggregator}/stop.aggregator")
        
    for sim_dir in sim_streams.keys():
        subprocess.Popen(["touch",f"{dir_simulations}/all/{sim_dir}/stop.simulation"])
        logging.info(f"Stopping simulation in {sim_dir} because of max_iterations = 0")
        subprocess.getstatusoutput(f"mv {dir_simulations}/running/{sim_dir} {dir_simulations}/stopped/{sim_dir}")
        sim_streams[sim_dir].close()

    logging.info(get_now())
    logging.info("Finished")


