from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
from pyiron_atomistics import Project, _StructureFactory
from pyiron_atomistics.atomistics.job.atomistic import AtomisticGenericJob
from pyiron_atomistics.atomistics.structure.atoms import Atoms
from pyiron_atomistics.lammps.lammps import Lammps as LammpsJob

from pyiron_contrib.workflow.channels import ChannelTemplate
from pyiron_contrib.workflow.node import Node


class BulkStructure(Node):
    input_channels = [
        ChannelTemplate(name="element", default="Fe", types=str),
        ChannelTemplate(name="cubic", default=False, types=bool),
        ChannelTemplate(name="repeat", default=1, types=int),
    ]
    output_channels = [
        ChannelTemplate(name="structure", types=Atoms),
    ]

    @staticmethod
    def engine(element, cubic, repeat):
        return {
            "structure": _StructureFactory().bulk(element, cubic=cubic).repeat(repeat)
        }


class Lammps(Node):
    input_channels = [
        ChannelTemplate(name="structure", types=Atoms),
    ]
    output_channels = [
        ChannelTemplate(name="job", types=LammpsJob),
    ]

    @staticmethod
    def engine(structure):
        pr = Project(".")
        job = pr.atomistics.job.Lammps("NOTAREALNAME")
        job.structure = structure
        job.potential = job.list_potentials()[0]
        return {"job": job}


class CalcMD(Node):
    input_channels = [
        ChannelTemplate(name="job_name", types=str),
        ChannelTemplate(name="job", types=(AtomisticGenericJob)),
        ChannelTemplate(name="n_ionic_steps", types=int, default=1000),
        ChannelTemplate(name="n_print", types=int, default=100),
        ChannelTemplate(name="temperature", types=(int, float), default=300.),
        ChannelTemplate(name="pressure", types=(float, tuple, type(None)), default=None)
    ]
    output_channels = [
        ChannelTemplate(name="steps", types=np.ndarray),
        ChannelTemplate(name="energy_pot", types=np.ndarray),
    ]

    @staticmethod
    def engine(job_name, job, n_ionic_steps, n_print, temperature, pressure):
        pr = Project("WORKFLOWNAMEPROJECT" + job_name)
        job = job.copy_to(project=pr, new_job_name=job_name, delete_existing_job=True)
        job.calc_md(
            n_ionic_steps=n_ionic_steps,
            n_print=n_print,
            temperature=temperature,
            pressure=pressure
        )
        job.run()
        return {"project": pr, "job": job}

    @staticmethod
    def postprocessor(project, job):
        steps = job.output.steps
        energy_pot = job.output.energy_pot
        job.remove()
        project.remove(enable=True)
        return {
            'steps': steps,
            'energy_pot': energy_pot
        }


class Plot(Node):
    input_channels = [
        ChannelTemplate(name="x", types=(list, np.ndarray)),
        ChannelTemplate(name="y", types=(list, np.ndarray)),
    ]
    output_channels = [
        ChannelTemplate(name="plot"),
    ]

    @staticmethod
    def engine(x, y):
        fig = plt.scatter(x, y)
        return {"plot": fig}
