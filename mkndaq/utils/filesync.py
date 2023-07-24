# %%
import os
import datetime
import time
import shutil
from filecmp import dircmp


# %%
def rsync(source: str, target: str, buckets: str = [None, "hourly", "daily"], days: int = 1, age: int = 3600) -> list:
    try:
        sep = os.path.sep
        if buckets=="hourly":
            fmt = f"%Y{sep}%m{sep}%d"
        elif buckets=="daily":
            fmt = f"%Y{sep}%m"
        elif buckets is None:
            fmt = None
        else:
            raise ValueError(f"'buckets' must be <None|hourly|daily>.")

        files_received = []
        now = time.time()

        if fmt:
            for day in range(days, 0, -1):
                dte = (datetime.datetime.now() - datetime.timedelta(days=day)).strftime(fmt)
                src = os.path.join(source, dte)
                if os.path.exists(src):
                    tgt = os.path.join(target, dte)
                    os.makedirs(tgt, exist_ok=True)
                    dcmp = dircmp(src, tgt)
                    for file in dcmp.left_only:
                        if (now - os.path.getmtime(os.path.join(src, file))) > age:
                            shutil.copy(os.path.join(src, file), os.path.join(tgt, file))
                            files_received.append(os.path.join(tgt, file))
                else:
                    print(f"'{src}' does not exist.")
        else:
            if os.path.exists(source):
                os.makedirs(target, exist_ok=True)
                dcmp = dircmp(source, target)
                for file in dcmp.left_only:
                    if (now - os.path.getmtime(os.path.join(source, file))) > age:
                        shutil.copy(os.path.join(source, file), os.path.join(target, file))
                        files_received.append(os.path.join(target, file))
            else:
                print(f"'{source}' does not exist.")

        return files_received

    except Exception as err:
        print(err)

# %%


    def store_and_stage_new_files(self):
        try:
            # list data files available on netshare
            # retrieve a list of all files on netshare for sync_period, except the latest file (which is presumably still written too)
            # retrieve a list of all files on local disk for sync_period
            # copy and stage files available on netshare but not locally
            
            if self._data_storage_interval == 'hourly':
                ftime = "%Y/%m/%d"
            elif self._data_storage_interval == 'daily':
                ftime = "%Y/%m"
            else:
                raise ValueError(f"Configuration 'data_storage_interval' of {self._name} must be <hourly|daily>.")

            try:
                if os.path.exists(self._netshare):
                    for delta in (0, 1):
                        relative_path = (datetime.datetime.today() - datetime.timedelta(days=delta)).strftime(ftime)
                        netshare_path = os.path.join(self._netshare, relative_path)
                        # local_path = os.path.join(self._datadir, relative_path)
                        local_path = os.path.join(self._datadir, time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"), relative_path)
                        os.makedirs(local_path, exist_ok=True)

                        # files on netshare except the most recent one
                        if delta==0:
                            netshare_files = os.listdir(netshare_path)[:-1]
                        else:
                            netshare_files = os.listdir(netshare_path)

                        # local files
                        local_files = os.listdir(local_path)

                        files_to_copy = set(netshare_files) - set(local_files)

                        for file in files_to_copy:
                            # store data file on local disk
                            shutil.copyfile(os.path.join(netshare_path, file), os.path.join(local_path, file))            

                            # stage data for transfer
                            stage = os.path.join(self._staging, self._name)
                            os.makedirs(stage, exist_ok=True)

                            if self._zip:
                                # create zip file
                                archive = os.path.join(stage, "".join([file[:-4], ".zip"]))
                                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                                    fh.write(os.path.join(local_path, file), file)
                            else:
                                shutil.copyfile(os.path.join(local_path, file), os.path.join(stage, file))

                            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} .store_and_stage_new_files (name={self._name}, file={file})")
                else:
                    msg = f"{time.strftime('%Y-%m-%d %H:%M:%S')} (name={self._name}) Warning: {self._netshare} is not accessible!)"
                    if self._log:
                        self._logger.error(msg)
                    print(colorama.Fore.RED + msg)

            except:
                print(colorama.Fore.RED + f"{time.strftime('%Y-%m-%d %H:%M:%S')} (name={self._name}) Warning: {self._netshare} is not accessible!)")

                return
                
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)

    # Methods below not currently in use

    def store_and_stage_latest_file(self):
        try:
            # get data file from netshare
            if self._data_storage_interval == 'hourly':
                path = os.path.join(self._netshare, time.strftime("/%Y/%m/%d"))
            elif self._data_storage_interval == 'daily':
                path = os.path.join(self._netshare, time.strftime("/%Y/%m"))
            else:
                raise ValueError(f"Configuration 'data_storage_interval' of {self._name} must be <hourly|daily>.")
            file = max(os.listdir(path))

            # store data file on local disk
            shutil.copyfile(os.path.join(path, file), os.path.join(self._datadir, file))

            # stage data for transfer
            stage = os.path.join(self._staging, self._name)
            os.makedirs(stage, exist_ok=True)

            if self._zip:
                # create zip file
                archive = os.path.join(stage, "".join([file[:-4], ".zip"]))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                    fh.write(os.path.join(path, file), file)
            else:
                shutil.copyfile(os.path.join(path, file), os.path.join(stage, file))

            print("%s .store_and_stage_latest_file (name=%s)" % (time.strftime('%Y-%m-%d %H:%M:%S'), self._name))

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def store_and_stage_files(self):
        """
        Fetch data files from local source and move to datadir. Zip files and place in staging area.

        :return: None
        """
        try:
            print("%s .store_and_stage_files (name=%s)" % (time.strftime('%Y-%m-%d %H:%M:%S'), self._name))

            # get data file from local source
            files = os.listdir(self._source)

            if files:
                # staging location for transfer
                stage = os.path.join(self._staging, self._name)
                os.makedirs(stage, exist_ok=True)

                # store and stage data files
                for file in files:
                    # stage file
                    if self._zip:
                        # create zip file
                        archive = os.path.join(stage, "".join([file[:-4], ".zip"]))
                        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                            fh.write(os.path.join(self._source, file), file)
                    else:
                        shutil.copyfile(os.path.join(self._source, file), os.path.join(stage, file))

                    # move to data storage location
                    shutil.move(os.path.join(self._source, file), os.path.join(self._datadir, file))

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
