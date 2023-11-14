from coffea import processor
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
from coffea.nanoevents.methods import candidate
import awkward as ak

from distributed import Client
from dask_jobqueue.htcondor import HTCondorCluster
import matplotlib.pyplot as plt
import uproot
import hist

processor.NanoAODSchema.warn_missing_crossrefs = False

class MyProcessor(processor.ProcessorABC):
    def __init__(self):
        dataset_axis = hist.axis.StrCategory(name="dataset", label="", categories=[], growth=True)
        e_axis = hist.axis.Regular(name="elePt", label="electron pT [GeV]", bins=45, start=15, stop=105)
        self._accumulator = processor.dict_accumulator({
            'cutflow': processor.defaultdict_accumulator(int),
            'hist_elePt': hist.Hist(dataset_axis, e_axis),
        })

    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        output = self.accumulator
        dataset = events.metadata['dataset']
        # Write the analysis here
        
        # the workflow can be define by: output['cutflow']['cut']
        # output['hist_elePt'].fill(dataset=dataset, elePt=good_electrons.pt)
        
        return output

    def postprocess(self, accumulator):
        return accumulator


cluster = HTCondorCluster(cores=1, memory="1GB", disk="1GB")
cluster.adapt(minimum=1, maximum=10)
client = Client(cluster)

output = processor.run_uproot_job(
    fileset={'data':["root://eospublic.cern.ch//eos/root-eos/cms_opendata_2012_nanoaod/ZZTo2e2mu.root"]},    # fileset used in analysis
    treename="Events",   # tree name in ROOT file
    processor_instance=MyProcessor(), 
    executor=processor.dask_executor, # Choose the suitable executor in your case.
    executor_args={
        "client": client,
        "schema": NanoAODSchema,
    },
)

# print(output['cutflow'])

# Draw the histogram
# fig, ax = plt.subplots(figsize=(6, 5))
# output['hist_elePt'].plot1d(ax=ax, color="orange", lw=3)
# plt.savefig("mZ.pdf")
