const { app, BrowserWindow } = require('electron')
const path = require('node:path')

function openWindow() {
  const window = new BrowserWindow({ title: 'Synch' })
  return window.loadFile(path.join(__dirname, 'dist', 'index.html'))
}

app.whenReady().then(openWindow)
app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) openWindow()
})
app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit()
})
