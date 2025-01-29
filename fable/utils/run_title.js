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


async function writeTitle(Runtime, filename) {
    const result = await Runtime.evaluate({
        expression: 'org.chromium.distiller.DomDistiller.apply()[1]'
    });
    let title = result.result.value;
    if (title == undefined) title = '';
    title = title.trim();
    fs.writeFileSync(filename, title);
}

async function startChrome(){
    const os = process.platform;
    assert(os == 'linux' | os == 'darwin')
    const path = os == 'linux' ? '/usr/bin/google-chrome' : '/Applications/Chromium.app/Contents/MacOS/Chromium'
    
    const debugPort = 9222;  // Fixed port
    
    let chromeFlags = [
        '--headless=new',
        '--disable-gpu',
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        `--remote-debugging-address=0.0.0.0`,  // Listen on all interfaces
        `--remote-debugging-port=${debugPort}`
    ];
    
    console.log('Starting Chrome with path:', path);
    console.log('Chrome flags:', chromeFlags.join(' '));
    
    try {
        const chrome = await chromeLauncher.launch({
            chromeFlags: chromeFlags,
            chromePath: path,
            startingUrl: process.argv[2],
            port: debugPort
        });
        
        console.log('Chrome started on port:', chrome.port);
        return chrome;
    } catch (err) {
        console.error('Failed to start Chrome:', err);
        process.exit(1);
    }
}

(async function(){
    const chrome = await startChrome();
    let filename = argv.filename
    const timeout = argv.timeout ? parseInt(argv.timeout) * 1000 : 30000;
    // let pidname = filename.substr(0, filename.length-5)

    let screenshot = argv.screenshot; 
    

    try {
        console.log('Connecting to Chrome debugging port...');
        const client = await CDP({
            host: 'localhost',
            port: chrome.port,
            target: (targets) => targets[0]  
        });        
        console.log('Connected to Chrome debugging port');
        
        const { Network, Page, Security, Runtime} = client;

        await Security.setIgnoreCertificateErrors({ ignore: true });

        await Network.enable();
        await Page.enable();

        Network.responseReceived((params) => {
            console.log(`Received response: ${params.response.status} ${params.response.url}`);
        });

        Network.loadingFailed((params) => {
            console.error('Loading failed:', params.errorText);
        });

        console.log('Navigating to URL:', process.argv[2]);
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
    
        await writeTitle(Runtime, filename);

    } catch (err) {
        console.error(err);
    } finally {
        if (typeof client !== 'undefined') client.close();
        await chrome.kill();
        process.exit(0);
    }
})()