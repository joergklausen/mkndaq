
# %%
import os
from filecmp import dircmp
import time


# %%
# test for aerosol
source = "P:/"
target = "C:/Users/mkn//Documents/mkndaq/data/aerosol"
age = 86400
files_to_stage = []

t0 = time.time()
left_only = dircmp(source, target).left_only

for file in left_only:
    if os.path.getmtime(os.path.join(source, file)) < (time.time() - age):
        files_to_stage.append(file)
print(time.time() - t0)

print(files_to_stage)


# %%
