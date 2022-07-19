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

In VS Code, open a terminal in your venv, then navigate to
	
``(venv) user@host:~/<path>/mkndaq$``
	
Execute

``pyinstaller --onefile --name mkndaq.exe mkndaq/mkndaq.py [--log-level=DEBUG]``
	
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

#### Meteo
Files are pushed by simple ftp to Minix and staged from there.

### References
- [0] https://realpython.com/pyinstaller-python/#customizing-your-builds
- [1] https://itsfoss.com/share-folders-local-network-ubuntu-windows
- [2] https://www.digitalcitizen.life/workgroup-ubuntu-linux/
