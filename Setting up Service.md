# Running a python program (or script) as a windows service

Use [Non-Sucking Service Manager](https://nssm.cc/download) to create the service. Once set up the application can be managed as any other windows services by running services.msc or by going to the Services tab from the Task Manager.

- Download [nssm](https://nssm.cc/download) and extract `nssm.exe`. It does not have any dependencies. Add the extracted dir path in %PATH% environment variable (preferably win64 dirpath) for simpler cmd access.
- Start Command Prompt (or Powershell) as an administrator. Right click on Command Prompt (or Powershell) and choose `Run as Administrator`.
- Following are the typical NSSM commands. Here, it is assumed that the script is using a python virtual environment. DO NOT USE RELATIVE PATH ANYWHERE IN THIS PROCESS.

1. Installing a service
``` cmd
nssm.exe install "SERVICE_NAME" "C:\project\venv\Scripts\python.exe" "C:\project\myscript.py"
```
For Finalyca worker. 
``` cmd
nssm install finalyca_worker "C:\_Finalyca\_finalyca\backend\venv\Scripts\python.exe" "C:\_Finalyca\_finalyca\backend\finalyca_worker.py"
```
> output should be something like `Service "SERVICE_NAME" installed successfully!`

2. Set log path
```
nssm set finalyca_worker AppStderr PATH_TO_ERROR_FILE.log
nssm set finalyca_worker AppStdout PATH_TO_LOG_FILE.log
```
> The file and path must exist. It will not be created automatically. Alternatively in-built UI could be used by following command. (Go under the tab 'I/O')
```
nssm edit finalyca_worker
```

3. Some more command
``` cmd
nssm.exe start finalyca_worker             /// start service
nssm.exe stop finalyca_worker              /// stop service
nssm.exe restart finalyca_worker           /// restart service
nssm.exe status finalyca_worker            /// service status
```

4. Remove a service
``` cmd
nssm.exe remove finalyca_worker
```

## Reference
- https://stackoverflow.com/questions/32404/how-do-you-run-a-python-script-as-a-service-in-windows
- https://medium.com/@m_ko/how-to-run-python-scripts-as-a-windows-service-979082706360
- https://python.plainenglish.io/turn-your-python-code-into-a-windows-service-in-3-steps-a109193d5ecc