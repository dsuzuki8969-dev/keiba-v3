Set ws = CreateObject("WScript.Shell")
Set fs = CreateObject("Scripting.FileSystemObject")
Set f = fs.OpenTextFile("C:\Users\dsuzu\keiba\keiba-v3\log\bat_trace.log", 8, True)
f.WriteLine "[" & Now & "] VBS_START"
f.Close
ec = ws.Run("cmd /c ""c:\Users\dsuzu\keiba\keiba-v3\scripts\daily_predict.bat""", 0, True)
Set f = fs.OpenTextFile("C:\Users\dsuzu\keiba\keiba-v3\log\bat_trace.log", 8, True)
f.WriteLine "[" & Now & "] VBS_END ec=" & ec
f.Close
WScript.Quit ec
