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
        mZ_axis = hist.axis.Regular(name="mZ", label="Z mass [GeV]", bins=30, start=60, stop=120)
        self._accumulator = processor.dict_accumulator({
            'cutflow': processor.defaultdict_accumulator(int),
            'hist_mZ': hist.Hist(dataset_axis, mZ_axis),
        })

    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        output = self.accumulator
        dataset = events.metadata['dataset']

        # Define the electron kinematics
        electrons = ak.zip({
            "pt": events.Electron.pt,
            "eta": events.Electron.eta,
            "phi": events.Electron.phi,
            "mass": events.Electron.mass,
            "charge": events.Electron.charge,
        }, with_name="PtEtaPhiMCandidate", behavior=candidate.behavior)
        
        # Select 2 good electrons with pT > 15 and |eta| < 2.4
        good_electrons = electrons[(electrons.pt > 15) & (abs(electrons.eta)<2.4)]
        pass_ele_selection = (ak.num(good_electrons) >= 2)
        
        # List all possible electron pairs in each event
        pair = ak.combinations(good_electrons, 2)
        
        # Reconstruct Z boson by 2 electrons with opposite charges and leading e pT > 25
        Z_selection = (
            (pair["0"].charge != pair["1"].charge) &
            (pair["0"].pt > 25)
        )
        Z_candidate = pair["0"][Z_selection]+pair["1"][Z_selection]
        pass_Z_selection = (ak.num(Z_candidate) >= 2)

        # apply the event selection
        pruned_ev = events[pass_ele_selection & pass_Z_selection]
        pruned_Z = Z_candidate[pass_ele_selection & pass_Z_selection]

        # choose permutation with Z mass closest to nominal Z boson mass
        Zidx = ak.singletons(ak.argmin(abs(pruned_Z.mass - 91.1876), axis=1))
        pruned_Z = ak.flatten(pruned_Z[Zidx])
        
        output['cutflow']['totalev'] += len(events)
        output['cutflow']['ele selection'] += ak.sum(pass_ele_selection)
        output['cutflow']['Z selection'] += ak.sum(pass_ele_selection & pass_Z_selection)
        output['hist_mZ'].fill(dataset=dataset, mZ=pruned_Z.mass)
        
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

print(output['cutflow'])
# Draw the histogram
fig, ax = plt.subplots(figsize=(6, 5))
output['hist_mZ'].plot1d(ax=ax, color="orange", lw=3)

plt.savefig("mZ.pdf")
