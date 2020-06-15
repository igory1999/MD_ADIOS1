import adios2
import time
import random
import numpy as np
import logging
from myutils import *
import os.path
import sys
import subprocess

class Simulation:
    def __init__(self, dir):
        self.dir = dir
        self.step = 0
        self._adios_stream = adios2.open(name=f"{dir}/SimulationOutput.bp", mode="w", config_file=f"{dir}/adios.xml", io_in_config_file="SimulationOutput") 
        self.stop = False
    def __del__(self):
        self._adios_stream.close()
    def produce(self):
        self.data = np.random.rand(3,2)
    def iterate(self):
        logging.info(f'In iterate: step = {self.step}')
        self.produce()
        logging.info(f'data = {self.data}')
        self.step += 1
        self._adios_stream.write("MyData", self.data, list(self.data.shape), [0,0], list(self.data.shape), end_step=True)
        self.qstop()
    def qstop(self):
        if(os.path.exists(f"{self.dir}/stop.simulation")):
           logging.info("Received kill signal")
           self.stop = True
    def set_stop(self):
        self.stop = True
    def run(self):
        while(not self.stop):
            logging.info("="*30)
            delay = random.uniform(0,3)
            logging.info(f'Sleeping for {delay:.1f}')
            time.sleep(delay)
            self.iterate()

if(__name__ == '__main__'):
    try:
        dir = sys.argv[1]
        adios_xml = sys.argv[2]
    except:
        print("Usage: python simulation.py dir adios_xml")
        sys.exit(1)
    subprocess.getstatusoutput(f'mkdir -p {dir}')
    subprocess.getstatusoutput(f'cp {adios_xml} {dir}/adios.xml')
    logging.basicConfig(filename=f'{dir}/simulation.log', filemode='w', level=logging.INFO)        
    logging.info("Start")
    logging.info(get_now())
    s = Simulation(dir)
    s.run()
    logging.info(get_now())
    logging.info("Finished")
    
