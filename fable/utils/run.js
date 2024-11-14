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


function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function writeHTML(Runtime, filename) {
    const result = await Runtime.evaluate({
        expression: 'document.documentElement.outerHTML'
    });
    const html = result.result.value;
    fs.writeFileSync(filename , html);
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
    let filename = argv.filename != undefined ? argv.filename : "temp"
    htmlname = `${filename}.html`;

    let screenshot = argv.screenshot; 
    
    fs.writeFileSync(htmlname, chrome.pid.toString());
    const client = await CDP({port: chrome.port});
    const { Network, Page, Security, Runtime} = client;
    // console.log(Security);

    try {
        await Security.setIgnoreCertificateErrors({ ignore: true });
        //Security.disable();

        await Network.enable();
        await Page.enable();

        await Page.navigate({ url: process.argv[2] });

        await Page.loadEventFired();
        
        // await sleep(5000);

        await writeHTML(Runtime, htmlname);

        if (screenshot){
            const pic = await Page.captureScreenshot({ format: 'jpeg'} );
            fs.writeFileSync(`${filename}.jpg`, pic.data);
        }

    } catch (err) {
        console.error(err);
    } finally {
        if (client){
            client.close();
            await chrome.kill();
            process.exit(0);
        }
    }

})()