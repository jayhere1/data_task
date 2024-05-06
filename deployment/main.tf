provider "aws" {
  region = "us-east-1"
}

data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"] # Canonical
}

resource "aws_instance" "web" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = "t3.micro"
  key_name = "test-ec2"  # To be created before deployment

  tags = {
    Name = "JSONProcessor"
  }
}
resource "aws_security_group" "app_sg" {
  name        = "json_data_processor_sg"
  description = "Security group for JSON data processor application"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # To be restricted for prod
  }

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "json_data_processor_sg"
  }
}

resource "aws_instance" "app_instance" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = "t2.micro"
  security_groups = [aws_security_group.app_sg.name]

  key_name = "test-ec2"  # To be created before deployment

  user_data = <<-EOF
              #!/bin/bash
              sudo apt-get update
              sudo apt-get install -y apt-transport-https ca-certificates curl software-properties-common
              curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
              sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
              sudo apt-get update
              sudo apt-get install -y docker-ce docker-compose
              sudo systemctl start docker
              sudo systemctl enable docker
              # Clone your repository and set up Docker Compose (Adjust as necessary)
              git clone https://github.com/jayhere1/data_task.git
              cd data_task
              sudo docker-compose up -d
              EOF

  tags = {
    Name = "JSON Data Processor Instance"
  }
}

output "public_ip" {
  value = aws_instance.app_instance.public_ip
}
