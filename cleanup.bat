REM cd ..
REM FOR /F "tokens=*" %%G IN ('DIR /B /AD /S bin ^| findstr /liv "node_modules"') DO ECHO "%%G"
REM FOR /F "tokens=*" %%G IN ('DIR /B /AD /S obj ^| findstr /liv "node_modules"') DO ECHO "%%G"
FOR /F "tokens=*" %%G IN ('DIR /B /AD /S bin ^| findstr /liv "node_modules"') DO RMDIR /S /Q "%%G"
FOR /F "tokens=*" %%G IN ('DIR /B /AD /S obj ^| findstr /liv "node_modules"') DO RMDIR /S /Q "%%G"