# mkndaq

@author: joerg.klausen@meteoswiss.ch

## scope
Data acquisition of MKN data and transfer to MeteoSwiss. Supported instruments are queried and all data are staged as
zip files for periodic transfer via sftp.

## using poetry
$ poetry add _"package"_ to add to the environment

### automation
To automate execution and to safeguard against crashes, configure Windows task scheduler to launch the application at
startup and every 5' thereafter. This is pre-configured and available as follows:
- Open 'Schedule tasks' control panel
- Import /dist/mkndaq.xml  (This is an export from the Task Scheduler task definition.)
- Verify that task is registered.

### Fallback
2025-12-26 / jkl

Installation of MKNDAQ as a fall-back on Picarro Windows 10 Pro

1. Install Windows git bash from https://git-scm.com/install/windows

2. Install Python 3.13 from https://www.python.org/ftp/python/3.13.11/python-3.13.11-amd64.exe
    - Use defaults, but then modify installation and pre-compile standard library, add Python to Environmet Variables

3. Clone git repository
    - Open CMD, create folder c:/users/picarro/git
    - git clone https://github.com/joergklausen/mkndaq.git

4. Create .venv
    - C:\Users\picarro>C:\Users\picarro\AppData\Local\Programs\Python\Python313\python -m venv c:\Users\picarro\git\mkndaq\.venv

5. Activate .venv
    - c:\Users\picarro\git\mkndaq\.venv\Scripts>activate
    
6. Install requirements.txt
    - cd c:/users/picarro/git/mkndaq
    - py -m pip install --upgrade pip
    - py -m pip install -r requirements.txt
    - py -m pip install polars[rtcompat]    # light polars version for older CPUs

7. Install private key
    - cd c:/users/picarro/
    - mkdir .ssh
    - transfer private key file 
    
8. Install S3 key
    - Create .aws, transfer key
    
9. Open Task Scheduler and import basic tasks
    - ../dist/mkndaq-fallback.xml
    - ../dist/fidas-fallback.xml

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
Alternatively, files may be pulled locally using scp.

scp admin@192.168.3.157:/home/moxa/data/VRXA00.* "%USERPROFILE%\Documents\mkndaq\data\meteo"

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

# Minix setup (v2026-01-13)
## Initial setup
- Set up a local administrator account 'admin' and a user account 'mkn'
- Configure BIOS (enter with F11 at boot):
    - Disable secure boot
    - Under 'Advanced > Restore on AC power loss', set to 'Power On'.
    - Set date and time, and timezone as UTC.
- Log into 'admin'
    - Install Windows 11 updates
    - Install Python 3.13.11
    - Install Notepad++
    - Install git for Windows
    - Install WinSCP
    - Install 7-zip
    - Install VS Code System-wide, and the following extensions:
        - Python with Pylance
        - autoDocstring
        - DataWrangler
    - Install VS Code extensions for Python, Pylance
    - Install MOXA UPort devices drivers (run find_serial_devices.py to identify devices on COM ports); configure devices for HMP110 sensors (RS4852W, 19200, N, 8, 1, no parity)

### References
- [0] https://realpython.com/pyinstaller-python/#customizing-your-builds
- [1] https://itsfoss.com/share-folders-local-network-ubuntu-windows
- [2] https://www.digitalcitizen.life/workgroup-ubuntu-linux/
