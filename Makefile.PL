use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    EXE_FILES           => [ 'pt' ],
    NAME                => 'File::Pairtree',
    AUTHOR              => 'John A. Kunze <jak@ucop.edu>',
    VERSION_FROM        => 'VERSION',
    ABSTRACT            => 'command and routines to manage pairtrees',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'BSD')
      : ()),
    PM			=> {
	'lib/File/Pairtree.pm'	=> 'blib/lib/File/Pairtree.pm'
    },
    PL_FILES            => {},
    PREREQ_PM         => {
	'Carp'		=> '0',
	'Pod::Usage'	=> '0',
	'Getopt::Long'	=> '0',
	'File::Find'	=> '0',
	'File::Path'	=> '0',
	'File::Glob'	=> '0',
	'File::OM'	=> '0.21',
	'File::Value'	=> '0.20',
	'File::Namaste'	=> '0.20',
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'File-Value-*' },
);
