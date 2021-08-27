# gaw-mkn-daq

@author: joerg.klausen@meteoswiss.ch

## mkndaq
Data acquisition of MKN data and transfer to MeteoSwiss. Supported instruments are queried and all data are staged as 
zip files for periodic transfer via sftp.

### creating a stand-alone executable
A stand-alone Windows 10 executable can be generated using pyinstaller [0]. 

In PyCharm, set up the PyInstaller workflow under File>Settings>Tools>External Tools>PyInstaller.
Use the following tool seetings:
- Program: C:\Users\jkl\Public\git\gaw-mkn-daq\venv\Scripts\pyinstaller.exe
- Arguments: --onefile --name mkndaq.exe $FilePath$
To execute the workflow:
- highlight the CLI script, __main__.py
- run >Tools>External Tools>PyInstaller.

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

#### TEI49I
TCP/IP communication. Specify formats in the config file.
- Align TCP/IP settings with what router provides, specifically
    - DHCP to OFF
	- choose IP
	- align subnet mask and gateway
	
#### Picarro G2401
Data collection from this instrument is currently handled simply through a network share.

### References
[0] https://realpython.com/pyinstaller-python/#customizing-your-builds
