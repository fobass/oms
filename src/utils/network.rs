use std::net::IpAddr;
use std::net::UdpSocket;

pub fn get_local_ip() -> Result<IpAddr, std::io::Error> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    let local_ip = socket.local_addr()?.ip();
    Ok(local_ip)
}
