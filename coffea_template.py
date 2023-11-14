from coffea import processor
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
from coffea.nanoevents.methods import candidate
import awkward as ak

from distributed import Client
from dask_jobqueue.htcondor import HTCondorCluster
import matplotlib.pyplot as plt
import uproot

processor.NanoAODSchema.warn_missing_crossrefs = False

class MyProcessor(processor.ProcessorABC):
    def __init__(self):
        self._accumulator = processor.dict_accumulator({
            'cutflow': processor.defaultdict_accumulator(int),
        })

    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        output = self.accumulator
        dataset = events.metadata['dataset']
        #* Write the analysis here...
 
        
        #* record the workflow: 
        output['cutflow']['totalev'] += len(events)
        
        return output

    def postprocess(self, accumulator):
        return accumulator


cluster = HTCondorCluster(cores=1, memory="1GB", disk="1GB")
cluster.adapt(minimum=1, maximum=10)
client = Client(cluster)

output = processor.run_uproot_job(
    fileset={'data':["root://eospublic.cern.ch//eos/root-eos/cms_opendata_2012_nanoaod/ZZTo2e2mu.root"]},    # fileset used in analysis
    treename="Events",   # tree name in ROOT file
    processor_instance=MyProcessor(), # Input the processor here
    executor=processor.dask_executor, # Choose the suitable executor in your case.
    executor_args={
        "client": client,
        "schema": NanoAODSchema,    # more options here: https://coffeateam.github.io/coffea/modules/coffea.nanoevents.html#classes
    },
)

#* show the cutflow
print(output['cutflow'])
