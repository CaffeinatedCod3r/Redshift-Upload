const electron = require('electron')
const app = electron.app
const browserWindow = electron.BrowserWindow

var mainWindow = null

app.on('window-all-closed', function () {
    app.quit()
})

app.on('ready', function () {

    var subpy = require('child_process').spawn('python3', ['./app.py'])
    var rq = require('request-promise')
    var mainAddr = 'http://localhost:5000'

    var openWindow = function () {
        mainWindow = new browserWindow({
            show: false,
            icon: 'app-icon.png'
        })
        mainWindow.maximize()
        mainWindow.setMenuBarVisibility(false)
        mainWindow.loadURL(mainAddr)
        mainWindow.show()

        mainWindow.on('closed', function () {
            mainWindow = null
            subpy.kill('SIGINT')
        })
    }

    var startUp = function () {
        rq(mainAddr)
            .then(function (htmlString) {
                openWindow()
            })
            .catch(function (err) {
                startUp()
            })
    }

    startUp()
})
