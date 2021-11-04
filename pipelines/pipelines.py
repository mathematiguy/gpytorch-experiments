import os
import yaml
import kfp.dsl as dsl
from runner import get_commit_hash
from kubernetes.client.models import V1EnvVar, V1VolumeMount

with open('credentials.yaml', 'r') as f:
    git_oauth_token = yaml.load(f)['github']['oauth_token']


class PipelineOp(dsl.ContainerOp):

    def __init__(self, name, job_dir, repo_url, git_branch, commit=None, requires=[],
                 params=None):
        self.name = name
        self.job_dir = job_dir
        self.repo_url = repo_url
        self.git_branch = git_branch
        self.requires = requires
        if not commit:
            self.commit = get_commit_hash()
        else:
            self.commit = commit
        self.params = params
        super().__init__(
            name=name,
            image=self.image,
            command='sh',
            arguments=['-c', ' && '.join(self.commands)],
            pvolumes={
                # Volume for containing finished job outputs
                "/output": self.output_dvop.volume},
            file_outputs={'output': '/output'}
        )

        self.create_job()

    @property
    def image(self):
        '''
        Set the docker container
        '''
        return self.config['docker']

    @property
    def config(self):
        '''
        Load job config from gorbachev.yaml file located in `job_dir`
        '''
        with open(os.path.join(self.job_dir, 'gorbachev.yaml'), 'r') as f:
            return yaml.load(f, Loader=yaml.FullLoader)

    @property
    def commands(self):
        '''
        Run these commands in the docker container
        '''
        return [
            # Create directories
            "mkdir -p /code",
            "mkdir -p /output",

            # Empty directories if there are any contents in the docker container
            "rm -rf /code/*",

            # Set working directory
            "cd /code",

            # Set git permissions using oauth token
            f'git config --global url."https://{git_oauth_token}@github.com".insteadOf https://github.com',
            f'git config --global url."https://{git_oauth_token}@github.com/".insteadOf git@github.com:',

            # Show commands as they are run
            "set -ex",

            # Recursively clone the git repo
            "git init",
            f"git pull {self.repo_url} {self.git_branch}",
            f'git checkout {self.commit}',
            "git submodule init",
            "git submodule update",

            # Run the code in the job directory
            f"cd {self.job_dir}",
            "bash build.sh"
        ]

    @property
    def output_dvop(self):
        '''
        Operation for creating the output persistent volume
        '''
        return dsl.VolumeOp(
            name=f"{self.name}-output",
            resource_name=f"{self.name}_output_pv",
            size="5Gi")

    def create_job(self):
        '''
        Finishes creating the job by:
          - Adding required jobs
          - Adding environment variables
          - Adding secrets
        '''

        if len(self.requires) > 0:
            self.add_required_jobs()

        if self.config.get('env'):
            self.add_environment_variables()

        if self.params.get('secrets'):
            self.add_secrets()

        self.limit_resources()

    def limit_resources(self):

        # Limit memory
        self.set_memory_limit(f"{self.params['memory']}G")

        # Limit cpus
        self.set_cpu_limit(str(self.params['cpus']))

        # Limit gpus
        gpu_devices = {
            '10gb': 'nvidia.com/mig-1g.10gb',
            '20gb': 'nvidia.com/mig-2g.20gb',
            '40gb': 'nvidia.com/mig-4g.40gb',
            '80gb': 'nvidia.com/mig-7g.80gb'
        }
        if self.params.get('gpus') and self.params.get('gpu_size'):
            self.add_resource_limit(gpu_devices[self.params['gpu_size']], self.params['gpus'])

    def add_required_jobs(self):
        '''
        Mounts the output volumes for required jobs under /input/`job_name` and
        Draws an arrow for the required job in the pipeline dag
        '''
        for required_job in self.requires:
            # Mount the output volume
            self.add_pvolumes(
                {f'/input/{required_job.name}': required_job.pvolumes['/output']}
            )
            # Draw the dag arrow
            self.after(required_job)

    def add_environment_variables(self):
        '''
        Adds environment variables to the container op
        '''
        if self.params:
            for var, value in self.params.get('env').items():
                self.add_env_variable(
                    V1EnvVar(name=var, value=value)
                )
        else:
            for var, value in self.config.get('env').items():
                self.add_env_variable(
                    V1EnvVar(name=var, value=value)
                )

    def add_secrets(self):
        '''
        Add secrets as environment variables
        TODO: replace this with k8s secrets
        '''
        for var, value in self.params.get('secrets').items():
            self.add_env_variable(
                V1EnvVar(name=var, value=value)
            )
