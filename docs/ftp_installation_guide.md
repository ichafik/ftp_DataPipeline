| Version  | Date |Author | Description
| ------ | ------ | -----| -----|
| `1.0` |  25/03/2020   |Imrane Chafik ♞ |Init|
| `1.1` ||||
# 1 Abstract:
This documentation would help you setting up FTP service locally on your linux local machine.
nothing is special with that, it will be very quick trust me ;)

# 2 Prerequisites:
Nothing but a linux running OS on your machine
# 3 Install FTP framework :
In this section there will be a plenty of command , by following them, you should be able to use your ftp service and connect to it using the famous opensource Filezilla.
* make sure Ubuntu is up-to-date:
``` sh 
sudo apt-get upgrade
```
* install vsftpd which is a GPL licensed FTP server for UNIX systems(including Linux). It is secure, extremely fast and  stable. 
``` sh
sudo apt-get install vsftpd
```
* install Filezilla
``` sh
sudo apt-get install filezilla
```


# 4 Configuring FTP server:
#### 4.1 Edit the CONF file:

open the vsftpd conf file 
``` sh
         # edit
        sudo vim /etc/vsftpd.conf
```
* remove "#" from the local_enable=YES
* remove "#" from the write_enable=YES
* remove "#" from the ascii_upload_enable=YES
* remove "#" from the ascii_download_enable=YES
* remove "#" from the user_sub_token=$USER
* remove "#" from the chroot_local_user=YES
* remove "#" from the chroot_list_enable=YES
* remove "#" from the chroot_list_file=/etc/vsftpd.chroot_list
* remove "#" from the local_root=/home/$USER/Public_html
* remove "#" from the allow_writeable_chroot=YES
* remove "#" from the ls_recurse_enable=YES

congratualtion now save what you edited 

#### 4.2 Edit the CHROOT file:

Open the Chroot file 

``` sh
         # edit
        sudo vim /etc/vsftpd.chroot_list
```
* Add username `Tester_FTP`
* Save and close
* Restart vsftpd
 ``` sh
         # ftp restart to let the conf take place
        sudo systemctl restart vsftpd
```
#### 4.3 Access to your server:
* Determine your ipadress:
``` sh
         # install net-tools
        sudo apt-get install net-tools
         # find your ipadress
        ifconfig 
``` 
* Forward a port on your router
 you'll need to forward your router's port 21 slot to that address; make sure that the port uses TCP
at this step you will also follow your router's instruction as this may varies from router to router


# Conclusion:
Congratulations now you have your ftp server set on your machine, and its up for your usage 


