#!/usr/bin/env python

import os
import sys
import subprocess
import json
import time

import radical.pilot as rp
import radical.utils as ru

if __name__ == '__main__':
    run = sys.argv[1]
    current_dir = os.getcwd()
    ADIOS_XML = f'{current_dir}/adios.xml'
    PYTHON = '/home/igor/.conda/envs/MD_ADIOS/bin/python'
    
    run_dir = f"{current_dir}/run_{run}"
    for d in ["new", "running", "stopped", "all"]:
        subprocess.getstatusoutput(f"mkdir -p  {run_dir}/simulations/{d}")
    subprocess.getstatusoutput(f'mkdir -p {run_dir}/aggregator')
        
    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    resource = 'local.localhost'
    session = rp.Session()

    try:
        config = ru.read_json(f'{current_dir}/config.json')
        pmgr   = rp.PilotManager(session=session)
        umgr   = rp.UnitManager(session=session)

        report.header('submit pilots')

        pd_init = {'resource'      : resource,
                   'runtime'       : 30,  # pilot runtime (min)
                   'exit_on_error' : True,
                   'project'       : config[resource]['project'],
                   'queue'         : config[resource]['queue'],
                   'access_schema' : config[resource]['schema'],
                   'cores'         : config[resource]['cores'],
                   'gpus'          : config[resource]['gpus'],
                  }
        pdesc = rp.ComputePilotDescription(pd_init)
        pilot = pmgr.submit_pilots(pdesc)

        n = 4  # number of units to run
        report.header('submit %d units' % n)

        umgr.add_pilots(pilot)

        simulation_counter = 0
        
        report.progress_tgt(n, label='create')
        cuds = list()
        for simulation_counter in range(0, n):
            cud = rp.ComputeUnitDescription()
            cud.name = f"simulation_{simulation_counter}"
            cud.executable    = PYTHON
            cud.arguments = [f'{current_dir}/simulation.py', f"{run_dir}/simulations/all/{simulation_counter}", ADIOS_XML]
            cud.pre_exec = [f'ln -s  {run_dir}/simulations/all/{simulation_counter} {run_dir}/simulations/new/{simulation_counter}']
            cud.cpu_processes = 1
            cuds.append(cud)
            report.progress()

        cud = rp.ComputeUnitDescription()
        cud.name = "aggregator"
        cud.executable =  PYTHON
        cud.arguments = [f'{current_dir}/aggregator.py',  current_dir, run_dir]
        cud.cpu_processes = 1
        cuds.append(cud)            
        report.progress_done()

        units = umgr.submit_units(cuds)

        running_simulations_units = list(filter(lambda x: x.description['name'].find('simulation') == 0, units))
        stopped_simulations_units = []
        aggregator_unit =  list(filter(lambda x: x.description['name'].find('simulation') == -1, units))[0]

        while(not os.path.exists(f"{run_dir}/aggregator/stop.aggregator")):
            cuds = list()
            move = []
            for u in running_simulations_units:
                if(u.state == rp.states.DONE):
                    move.append(u)
                    stopped_simulations_units.append(u)
                    simulation_counter += 1
                    cud = rp.ComputeUnitDescription()
                    cud.name = f"simulation_{simulation_counter}"
                    cud.executable    = PYTHON
                    cud.arguments = [f'{current_dir}/simulation.py', f"{run_dir}/simulations/all/{simulation_counter}", ADIOS_XML]
                    cud.pre_exec = [f'ln -s  {run_dir}/simulations/all/{simulation_counter} {run_dir}/simulations/new/{simulation_counter}']
                    cud.cpu_processes = 1
                    cuds.append(cud)
                    report.progress()
            for u in move:
                 running_simulations_units.remove(u)
            if(len(cuds)>0):
                units = umgr.submit_units(cuds)
                running_simulations_units += units
            time.sleep(5)

        umgr.wait_units()
    except Exception as e:
        report.error('caught Exception: %s\n' % e)
        ru.print_exception_trace()
        raise

    except (KeyboardInterrupt, SystemExit):
        ru.print_exception_trace()
        report.warn('exit requested\n')

    finally:
        report.header('finalize')
        session.close(download=True)

    report.header()

