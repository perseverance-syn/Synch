// Preload: exposes a small bridge (window.synch) to the isolated renderer.
// It forwards to main-process IPC handlers only; no Node API reaches the page and the token stays in main.
const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('synch', {
  getSettings: () => ipcRenderer.invoke('settings:get'),
  saveSettings: (url, token) => ipcRenderer.invoke('settings:save', { url, token }),
  forgetToken: () => ipcRenderer.invoke('settings:forget'),
  request: (req) => ipcRenderer.invoke('api:request', req),
  openStream: (stream, after) => ipcRenderer.invoke('stream:open', { stream, after }),
  closeStream: (handle) => ipcRenderer.invoke('stream:close', handle),
  onFrame: (cb) => {
    const listener = (_e, frame) => cb(frame)
    ipcRenderer.on('stream:frame', listener)
    return () => ipcRenderer.removeListener('stream:frame', listener)
  },
})
