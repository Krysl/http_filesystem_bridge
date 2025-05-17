# HTTP FileSystem Bridge （For Windows Only）(WIP)

This is a Rust practice project. The main functionality was implemented within a day, so it’s a bit rudimentary.
The code is based on the [memfs example](https://github.com/dokan-dev/dokan-rust/tree/master/dokan/examples/memfs) from [Dokan-rust](https://github.com/dokan-dev/dokan-rust).

The motivation behind developing this project is that I wanted to use Flutter to develop a Chrome extension. However, as we all know, Chrome extensions can only load local folders and cannot load files directly from Flutter's development server. This prevents me from using Flutter's hot-reload feature, which is quite frustrating.

Initially, I thought about modifying the flutter-tools development tool, but that seemed too complicated.

Later, I realized that if I had a bridge between HTTP and the file system, it might solve the problem —— disguising files on an HTTP server as local file system files accessible to Chrome.

My knowledge of Rust is still at a beginner level, and much of the code was modified with the help of GitHub Copilot. There are probably many issues with it, but at least Flutter can now run in Chrome through the local file system, which is quite interesting.

## Install 

1. Install [Dokan](https://github.com/dokan-dev/dokany/releases)
2. Download the released exe </br>
   Or Clone this repository and build

## Usage

```
# http_fs.exe --help
Usage: http_fs.exe [OPTIONS] --mount-point <MOUNT_POINT> --url <URL> --dir_tree <DIR_TREE>

Options:
  -m, --mount-point <MOUNT_POINT>  Mount point path.
  -u, --url <URL>                  http url.
  -j, --dir_tree <DIR_TREE>        dir tree in json format.
  -i, --fs-ignore[=<BOOL>]         ignore files using .fsignore .ignore or .gitignore. [default: false] [possible values: true, false]
  -t, --single-thread              Force a single thread. Otherwise Dokan will allocate the number of threads regarding the workload.
  -d, --dokan-debug                Enable Dokan's debug output.
  -r, --removable                  Mount as a removable drive.
  -h, --help                       Print help

```

#### example
`set RUST_LOG=error && cargo run -- --mount-point Z: -u http://localhost:5223  -j tree.json -i`
Parameter explanation:

1. `--mount-point Z:`: 

    <img width="200" alt="Image" src="https://github.com/user-attachments/assets/b9e23cd1-8ead-4570-9bb5-75c3165be30c" />

2. `-j tree.json`
    This option is used to generate the default directory.

    Using a JSON file, show the directory structure 
    (Currently only simple directories are supported, and directories need to end with a `/`.)


    ```json
    {
      "name": "/",
      "children": [
        {
          "name": "_locales/",
          "children": [
            {
              "name": "en/",
              "children": []
            },
            {
              "name": "zh/",
              "children": []
            },
            {
              "name": "zh_CN/",
              "children": []
            }
          ]
        }
      ]
    }
    ```

3. `-i`
    Enable file ignoring.

    This memory file system will, by default, create any file accesses to the virtual file system and then attempt to download the files from the HTTP server. 

    Some background processes may also access files that do not actually exist, which could be a waste of CPU and network bandwidth. 

    Therefore, files that are not needed can be blocked using a syntax similar to gitignore.

    Additionally, some programs may behave differently due to differences in directory structure. For example, a Chrome extension may attempt to access the _metadata/ folder, which can be blocked as needed.

    ```gitignore
    .git/
    .git
    .git.*
    refs
    refs.*
    config
    objects
    HEAD
    commondir
    AutoRun.inf
    autorun.inf
    _metadata/
    _metadata/*
    *.exe
    *.lnk
    ```

#### use with flutter web(wasm) developing
1. run in flutter project first: (js version is too slow for loading the .js files)

    (DO not close the chrome window launched by flutter)

    `flutter run -d chrome --wasm  --web-browser-flag "--disable-web-security" --web-port=5223`

    or using `launch.json`:

    ```json
    {
      "version": "0.2.0",
      "configurations": [
        {
          "name": "test",
          "request": "launch",
          "type": "dart",
          "args": [
            "-d",
            "chrome",
            "--wasm",
            "",
            "--web-browser-flag",
            "--disable-web-security",
            "--web-port=5223"
          ]
        },
      ]
    }
    ```

2. start the bridge using the same port

    `http_fs.exe --mount-point Z: -u http://localhost:5223 -j tree.json -i`

3. open the `file:///Z:/index.html` in chrome launched by Flutter 

    (if you close the chrome folder ,you can launch it by 

    `"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe" --disable-web-security  --user-data-dir="[some directory here]"`)

4. enjoy the hot-reload

