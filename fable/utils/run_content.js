/* 
    Run the chrome to load the page
    Usage: node run.js url --filename filename --screenshot
    filename and screenshot is optional
    Save the chrome's pid into filename.html for easy killing
    If screenshot is eanbled, save the screenshot as filename.jpg in Base64!! Require decoding to see
*/

const CDP = require('chrome-remote-interface');
const fs = require('fs')
const parse = require('url').parse
const chromeLauncher = require('chrome-launcher');
const assert = require('assert');
const argv = require('yargs').argv;



async function writeContent(Runtime, filename) {      
    const result = await Runtime.evaluate({
        expression: 'org.chromium.distiller.DomDistiller.apply()[2][1]'
    });
    let content = result.result.value;
    if (content == undefined) content = ''
    fs.writeFileSync(filename, content);
}

async function startChrome(){
    const os = process.platform;
    assert(os == 'linux' | os == 'darwin')
    const path = os == 'linux' ? '/opt/google/chrome/chrome' : '/Applications/Chromium.app/Contents/MacOS/Chromium'
    
    let chromeFlags = [
        '--disk-cache-size=1', 
        '-disable-features=IsolateOrigins,site-per-process',
    ];

    if (process.env.ROOT_USER) {
        chromeFlags.push('--no-sandbox');
    }
    
    if (os == 'linux') chromeFlags.push('--headless')
    const chrome = await chromeLauncher.launch({
        chromeFlags: chromeFlags,
        chromePath: path,
        // userDataDir: '/tmp/nonexistent' + Date.now(), 
    })
    return chrome;
}


(async function(){
    const chrome = await startChrome();
    let filename = argv.filename
    const timeout = argv.timeout ? parseInt(argv.timeout) * 1000 : 10000;
    // let pidname = filename.substr(0, filename.length-5)

    let screenshot = argv.screenshot; 
    

    try {
        const client = await CDP({port: chrome.port});
        const { Network, Page, Security, Runtime} = client;

        await Security.setIgnoreCertificateErrors({ ignore: true });
        //Security.disable();

        await Network.enable();
        await Page.enable();

        await Page.navigate({ url: process.argv[2] });
        
        await Promise.race([
            Page.loadEventFired(),
            new Promise(function(_, reject) {
                setTimeout(function() {
                    reject(new Error(`run_content.js: Timeout after ${timeout}`));
                }, timeout);
            }),
        ]).catch(function(err){
            console.log(err.message)
        })

        await Promise.race([
            writeContent(Runtime, filename),
            new Promise(function(_, reject) {
                setTimeout(function() {
                    reject(new Error(`run_content.js: Got content timeout`));
                }, 3000);
            }),
        ]).catch(function(err){
            console.log(err.message)
            fs.writeFileSync(filename, '');
        })

    } catch (err) {
        console.error(err);
    } finally {
        if (typeof client !== 'undefined') client.close();
        await chrome.kill();
        process.exit(0);
    }
})()