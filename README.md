# mkndaq

@author: joerg.klausen@meteoswiss.ch

## scope
Data acquisition of MKN data and transfer to MeteoSwiss. Supported instruments are queried and all data are staged as 
zip files for periodic transfer via sftp.

### creating a stand-alone executable
A stand-alone Windows 10 executable can be generated using pyinstaller [0]. 

In PyCharm, set up the PyInstaller workflow under File>Settings>Tools>External Tools>PyInstaller.
Use the following tool seetings:
- Program: <path>\gaw-mkn\venv\Scripts\pyinstaller.exe
- Arguments: --onefile --name mkndaq.exe $FilePath$
To execute the workflow:
- highlight the CLI script, __main__.py
- run >Tools>External Tools>PyInstaller.

In VS Code, open a bash terminal, then activate your .venv, then navigate to /mkndaq

``source .venv/Scripts/activate``	
``(.venv) user@host:~/<path>/mkndaq$``
	
Execute

``pyinstaller --onefile --name mkndaq.exe ./mkndaq/mkndaq.py``

or (with logging)

``pyinstaller --onefile --log-level=DEBUG --name mkndaq.exe ./mkndaq/mkndaq.py``
``pyinstaller --onefile --log-level=DEBUG --name tei49x.exe ./tei49x.py``

### usage
Execute in a command window as

``mkndaq [-s] -c <path to mkndaq.cfg>``.

### automation
To automate execution and to safeguard against crashes, configure Windows task scheduler to launch the application at 
startup and every 5' thereafter. This is pre-configured and available as follows:
- Open 'Schedule tasks' control panel
- Import /dist/mkndaq.xml
- Verify that task is registered. 

### Supported instruments
#### TEI49C
Serial communication using RS-232. Specify formats in
the config file.

#### Thermo Model 49I
TCP/IP communication. Specify formats in the config file.
- Align TCP/IP settings with what router provides, specifically
    - DHCP to OFF
	- choose IP
	- align subnet mask and gateway

#### Picarro G2401
Hourly files are pushed from Picarro to Minix using simple ftp. Files are staged from there. 
(NB: There is also a file share setup on the MKN MINIX. Share is not needed for mkndaq, but may be useful anyway).

#### Aerosol rack
Hourly files are pushed from PSI NUC to Minix using simple ftp. 
See [1,2]. Enable sharing of /Data/send as 'psi'. Lookup IP address using ifconfig. (NB: Share is not needed for mkndaq, but may be useful anyway).

- Identify workgroup on Windows 10 machine:
    - net config workstation

- Identify and set workgroup on Ubuntu machine
    - sudo gedit /etc/samba/smb.conf
    - if needed, change the default entry 'workgroup = WORKGROUP' to match the Windows workgroup

Files are now also pushed by simple ftp to Minix and staged from there.

Update: Currently, data from /Data/sent are accessible as a netshare (mounted disk) on the Minix.

#### Meteo
Files are pushed by simple ftp to Minix and staged from there.

#### Acoem NE-300
Full control

#### Aurora 3000
n/a

##### Firmware Release version: 1.39.000 (https://www.acoem.com/australasia/old-environmental-monitoring/aurora-firmware/)
Release date: 14/08/2019
Description of changes :
- Improved timing and memory configuration to improve instrument stability
- All readings from each measurement cycle can be logged. (i.e. every 3 seconds)
- 1-minute data is now an average over the last clock minute (i.e. 12:50 to 12:51)
- 5-minute data is the average of the last five one-minute averages
- Sigmas, Measure Ratios or Counts can be Logged internally
- Supports the Internal MFC Option
- Supports Airodis 5.1.4 Demo version
- No longer supports the Aurora Data Downloader. (!!)
Note: When installing this new firmware Version, the Aurora 3000 may freeze for 20 minutes while it re configures the memory. Do not turn off.


### References
- [0] https://realpython.com/pyinstaller-python/#customizing-your-builds
- [1] https://itsfoss.com/share-folders-local-network-ubuntu-windows
- [2] https://www.digitalcitizen.life/workgroup-ubuntu-linux/
