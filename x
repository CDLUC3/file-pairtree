#!/usr/bin/perl -w -Ilib

# XXX add to spec: two ways that a pairpath ends: 1) the form of the
# ppath (ie, ends in a morty) and 2) you run "aground" smack into
# a "longy" ("thingy") or a file

# xxx other stats to gather: total dir count, total count of all things
# that aren't either reg files or dirs; plus max and averages for all
# things like depth of ppaths (ids), depth of objects, sizes of objects,
# fanout; same numbers for "pairtree.*" branches

use strict;
use File::Find;
use File::Pairtree qw( $pair $pairp1 $pairm1 );

my $R = $File::Pairtree::root;

# Set up a big "find" expression that depends on these features of GNU find:
#   -regex for high-functioning, wholepath matching
#   -printf to tag files/dirs found with certain characteristics
#
# The basic idea is to walk a given hierarchy and tag stuff that looks
# like an object.  Mainstream objects are encapsulated in directory names
# of three characters or more, but we still have to detect the edge cases.
# All candidate object cases are printed on a line with the pairpath
# (ppath) first (as primary sort field), the tagged case, and the file/dir
# name found at the end of the ppath.
#
# XXXXXXXX better tags needed
#	NS=Non-Shorty directory (normal object)
#	UF=Unencapsulated File (encaps. warning),
#	PM=Post-Morty Shorty or Morty encountered (encaps. warning)
#	UG=Unencapsulated Group (encaps. warning)
#       EP=Empty Pairpath (indicator)
#
# The output of the 'find' is sorted (important) so that leaves descending
# from a given ppath cluster in groups.  The resulting groups are used to
# figure out how best to detect and repair any encapsulation problems.
# We offer xxx to repair encapsulation problems because they're non-trivial
# to detect (ie, there will be pairtree walkers that don't detect them) and
# we want to encourage proper encapsulation for the sake of interoperability.
#
# One odd case is an object right at the root of a pairtree, which means
# an empty path, hence an empty identifier.  Because systems frequently
# reserve special meaning for an empty or root value, and they/we might
# want to put something at that special location (eg, an object describing
# the pairtree), we will detect and count it as an object; its meaning and
# (il)legality is up to the implementor.  This has the nice side-effect
# that we'll have no fatal errors in processing a pairtree.
#
# XXX do edge case of pairtree_root/foo.txt
# XXX what to do with symlinks? and unusual filenames?

# Set $verbosefind to '-print' to show everything that 'find' handles,
# but normally don't show by setting it to '-true'.
my $verbosefind='-print';
#my $verbosefind = '-true';

# Normally prune for speed.  Set $verbosefind='-print' and noprune='-true'
# to see what processing steps would happen if you don't prune. xxx
my $noprune='-true';
#my $noprune = '-prune';

# This matches the base ppath in 'find'.
my $PP = '\([^/][^/]/\)*[^/][^/]?';

# This matches the base ppath in 'perl'.
my $P = "([^/]{$pair}/)*[^/]{1,$pair}";

my $tree = $ARGV[0];

$| = 1;		# XXXX unbuffer output   

my $irregularcount = 0;		# non file, non dir fs items to report xxx
my ($dev, $ino, $mode, $nlink, $uid, $gid, $rdev, $sze);

my $in_object = 0;

sub prenode{

	return () if (scalar(@_) == 0);		# no work if no items
	return @_ if ($in_object);		# no-op if inside object
	my @ground = ();
	my @objdirs = ();
	for (@_) {
		($dev, $ino, $mode, $nlink, $uid, $gid, $rdev, $sze)
			= stat($_);
		#if (m@^[^/]{$pairp1,}@ || S_ISREG($mode)) {	# xxx efficiency?
		if (m@^[^/]{$pairp1,}@ || -f $_) {
			push(@ground, $_);
		}
		elsif (-d $_)  {
			push(@objdirs, $_);
		}
		else {		# nothing else will be processed
			$irregularcount++;
		}
	}
	print("Ground files: ", join(", ", @ground), "\n")
		if (scalar(@ground) > 0);
	push @ground, sort(@objdirs);
	return @ground;
}

my $wpname = '';		# whole pathname
my $tpname = '';		# tail of name
my $cdname = '';		# current directory name

my %curobj = ( 'ppath' => '', 'encaperr' => 0, 'octets' => 0, 'streams' => 0 );

my @ppstack = ();

sub mkstackent{ my( $ppath )=@_;
	return { 'pp' => $ppath, 'bytes' => 0, 'items' => [],
			'objtype' => 1, 'flag' => 0 };
}

my ($ci_wpname, $ci_ppath, $ci_octets, $ci_streams);
$ci_ppath = '';

push(@ppstack, mkstackent(''));
my $top;
my $oldcdname = '';
my $symlinks_followed = 1;			# to minimize lstat calls
my $follow_fast = 1;	# follow symlinks without rigorous checking
			# also means that (-X _) works without stat
#$symlinks_followed = $follow_fast = 0;		# xxx make option-controled

find({ wanted => \&visit, follow_fast => $follow_fast }, $tree);

sub visit{	# receives no args

	$cdname = $File::Find::dir;		# current parent directory name
	$tpname = $_;				# current filename in that dir
	$wpname = $File::Find::name;		# complete pathname to file
	($dev, $ino, $mode, $nlink, $uid, $gid, $rdev, $sze) = lstat($tpname)
		unless ($symlinks_followed);	# else lstat done for us already
	print "NEXT: $cdname $_ $wpname\n";

	# Node invariant:  upon entry compare parent to stack top and pop
	# stack (unvisiting each node) until top equals current parent.
	# Lstat will always have been called before the real work begins,
	# regardless of whether symlinks are being followed.
	#
	# Directory node invariant:  upon dir exit, current wpname, if it's a
	# directory, must be pushed on stack.
	# 
	# Check our current ancestory and pop the stack as needed
	# until stack top equals the parent for this (current) node.
	# If there's a current item (open), we have to check for item
	# boundaries; if we pop back through an item name, we need to
	# close it.
	#
	$top = $ppstack[$#ppstack];	# xxx is this $#... safe?
	if ($cdname eq $top->{'pp'}) {		# nothing to pop
		print "descendant $_\n";	# xxx now what?
		#push(@{$top->{'items'}}, $_);
		#$top->{'flag'} |= 1;
	}
	elsif ($cdname =~ m@^$top->{'pp'}@) {
		die("find unexpectedly jumped more than one level deeper"
				. ": ppath=$top->{'pp'}, cdname=$cdname")
			if ($top->{'pp'} ne '');
		# else still initializing (first visit), so fall through
	}
	else {
		do {
			# XXXXX much reporting during "unvisit"
			unvisit(pop(@ppstack));
			$top = $ppstack[$#ppstack];	# xxx $#... safe?
		} until ($cdname eq $top->{'pp'});
	}

	# If we get here, the stack top is the same as our parent.
	#
	if (-l _) {
		print "XXXX SYMLINK $_\n";
		# XXX what does this branch do when _not_ following links?
		($dev, $ino, $mode, $nlink, $uid, $gid, $rdev, $sze)
			= stat($tpname);		# get the real thing
			# get type that symlink points to
	}

	# If here, we've done a stat for the actual file or dir.
	#
	if (-f _) {

		# Regular File Branch.
		#
		# Every file belongs to some item.  Every item is either an
		# object or, if improper encapsulation, part of an object.
		# If you encounter a file and there's no 'current' item,
		# then it's not properly encapsulated; else, your file
		# gets counted as part of the current item's stats.
		#
		#xxxxx put this back in _after_ processing node (during
		# unvisit:  if ($wpname =~ m@^.*$R/(.*/)?pairtree.*$@) {
		#	-prune
		#}
		if ($wpname =~ m@^.*$R/$P/[^/]+$@) {
			#print "m@.*$R/$P/[^/]+@: $_\n";
			# yyy add item to stacked object top level,
			#     flag encap err
			# yyy add size to stacked object
		 	print "$cdname UF $tpname\n";
		#	-fprintf $altout 'UF %h %s\n'
		}
		else {
			# yyy add size to stacked object
			$curobj{'bytes'} += (-s _);
			$curobj{'streams'}++;
			#print "cobjbytes=$curobj{'bytes'}, cobjstreams=",
			#	$curobj{'streams'}, "\n";
		#	-fprintf $altout 'IN %p %s\n'
		#	$noprune
		}
		return;
	}
	elsif (! -d _) {

		# Non-regular file, non-directory Branch.
		#
		$irregularcount++;
		# xxxx can't under follow_fast, _ caches stat results; can't I
		# get the file types (to count)  without doing another stat?
		return;
	}

	# Directory (or symlink) Branch.
	#
	# If we're here we know that we have a directory (-d _).

	# Now, look at the form of pathname.
	#
	#xxxxx put this back in _after_ processing node (during
	# unvisit: if ($wpname =~ m@^.*$R/(.*/)?pairtree.*$@) {
	#	-prune
	#	$top = mkstackent($cdname);
	#	push(@ppstack, $top);
	#}

	# XXX add re qualifier so Perl knows re's not changing
	# if we've hit what might be a regular object dir...
	if ($wpname =~ m@^.*$R/($P/)?[^/]{$pairp1,}$@) {

		# We're at an item directory; hopefully it's a properly
		# encapsulated object, but we won't know until all of its
		# peers have been seen.  So we "start" new current item
		# after first closing any previously open item.  It is a
		# fatal error if any previous item is either not at the
		# same level or not closed (fatal because our assumptions
		# about the algorithm may be wrong).
		#
		if ($ci_ppath eq '') {		# previous item was closed
			$ci_ppath = $cdname;
		}
		elsif ($ci_ppath eq $cdname) {	# still open at the same level

			# Need to close previous item and store on stack
			push(@{$top->{'items'}}, {	# push, later shift
				'ppath' => $ci_ppath,
				'wpname' => $ci_wpname,
				'octets' => $ci_octets,
				'streams' => $ci_streams,
			});
		}
		else {
			die("in $cdname, previous item '$ci_wpname' "
				. "wasn't closed");
		}

		# Initialize new item.  $ci_ppath already set correctly.
		#
		$ci_wpname = $wpname;
		$ci_octets = $ci_streams = 0;

		$top = mkstackent($wpname);	# xxx PM?
		push(@ppstack, $top);

		# yyy compare cdname to stack top.
		#     if cdname is same as stack top, {add item
		#     to list contained in stack top, flag encaperr}
		#     elsif cdname is not superstring of stack top {
		#     we've just closed off a ppath and we need
		#     to pop the stack top and report (a) proper
		#     or improper encapsulation (#items > 1 or
		#     any file item) and (b) accumulated oxum (if
		#     no items, report EP empty ppath.}
		#     In any case, push curr ppath as new stack
		#     top and add item to list at stack top, but
		#     flag encap err if PM err)
		#     (at end, report stack top)
		# start new object; but end previous object first
		# form: ppath, EncapErr, bytes, streams
		print "$cdname NS $tpname\n";
		#	-fprintf $altout 'START %h 0\n'
		#	$noprune
	}
	elsif ($wpname =~ m@^.*$R/$P$@) {

		# Extending the ppath, no item impact.
		#
		$top = mkstackent($wpname);
		push(@ppstack, $top);

		# yyy see above
		#	-empty
		#	-printf '%p EP -\n'
	}
	# $pair, $pairm1, $pairp1
	elsif ($wpname =~
	    m@^.*$R/([^/]{$pair}/)*[^/]{1,$pairm1}/[^/]{1,$pair}$@) {

		# We have a short directory following the end of a ppath.
		# This means a Post-Morty warning and starts a new item.

		# yyy [combine with NS regexp and do similarly???]
		# xxx push dir node
	XXXXXXXXXXXXX check and close any current item
		print "$cdname PM $tpname\n";
		$top = mkstackent($wpname);
		#push(@{$top->{'items'}}, $_);
		push(@ppstack, $top);
		#	-fprintf $altout 'START %h 0\n'
		#	$noprune
	}
	else {
		$top = mkstackent($cdname);
		push(@ppstack, $top);
	}

	return;
}

sub unvisit{ my( $top )=@_;

	die "can't unvisit undefined stack node"
		if (! defined($top));
	# if stack top eq current item, close out item
	if $top->{'pp'} eq 
	# xxxx print cur_item
	# xxxx if in item...
	print "unvisiting $top->{'pp'}, objtype=$top->{'objtype'}, ",
		"item(s)=", join(", ", @{$top->{'items'}}),
		", size=$top->{'bytes'}, ", "flag=$top->{'flag'}\n";
	for (@{$top->{'items'}}) {
		print $_->{'wpname'}, $_->{'octets'} . "." . $_->{'streams'};
	}
	return;
}

sub postnode{
	return;
}

sub newptobj{ my( $ppath, $encaperr, $bytes, $streams )=@_;

	if ($curobj{'ppath'}) {		# print record of previous obj
		print "id: $curobj{'ppath'}, $curobj{'encaperr'}, $curobj{'bytes'}.$curobj{'streams'}\n";
	}
	die("newptobj: all args must be defined")
		unless (defined($ppath) && defined($encaperr)
			&& defined($bytes) && defined($streams));
	$curobj{'ppath'} = $ppath;
	$curobj{'flag'} = $encaperr;
	$curobj{'bytes'} = $bytes;
	$curobj{'streams'} = $streams;
}

exit(0);

# /dev/stderr seems to be the only file name you can give to the fprintf
# action of 'find' so output from different clauses will be correctly
# interleaved.  We assume that stderr will be closed and all output
# flushed by the time the sort is finished, so when later we read
# both outputs, we won't get ahead of things. xxx say this better
#
my $altout = '/dev/stderr';

# Test for .*$R/(.*/)?pairtree.* must occur early.
#
# xxx report null: for pairtree.* case? and possibly size?
my $findexpr = qq@$verbosefind , \\
	-regex ".*$R/\\(.*/\\)?pairtree.*" \\
		-prune \\
	-o \\
	-type d \\
		-regex ".*$R/\\($PP/\\)?[^/][^/][^/]+" \\
		-printf '%h NS %f\\n' \\
		-fprintf $altout 'START %h 0\\n' \\
		$noprune \\
	-o \\
	-type d \\
		-regex ".*$R/$PP" \\
		-empty \\
		-printf '%p EP -\\n' \\
	-o \\
	-type f \\
		-regex ".*$R/$PP/[^/]+" \\
		-printf '%h UF %f\\n' \\
		-fprintf $altout 'UF %h %s\\n' \\
	-o \\
	-type d \\
		-regex ".*$R/\\([^/][^/]/\\)*[^/]/[^/][^/]?" \\
		-printf '%h PM %f\\n' \\
		-fprintf $altout 'START %h 0\\n' \\
		$noprune \\
	-o \\
	-type f \\
		-fprintf $altout 'IN %p %s\\n' \\
		$noprune \\
@;

#XXXXX yuck.  I may not be able to size improperly unencapsulated files
#  with 'find'

# The -type f test to get filesizes should occur after the UF file test
# XXX move up to first test? for efficiency?

#print "findexpr=$findexpr\n";

# xxx change pt_z to a mktemp, in case two scans are going at once
my $szfile = 'pt_z';

open(FIND, "find $tree $findexpr 2>$szfile | sort |")
	|| die("can't start find");

open(SIZES, "< $szfile") || die("can't open size file");

my $defsize = '(:unas)';		# xxx needed?
my ($sztype, $which, $size) = ('', '', 0);
my ($ptbcount, $ptfcount) = (0, 0);

sub getsizeline{
	$_ = <SIZES>;
	die("Error: unexpected size line format: $_")
		if (! /^(\S+) (\S+) (.*)/);
	return ($1, $2, $3);		# $sztype, $which, $size
}

sub getsize{ my( $ppath )=@_;

	my ($ppbcount, $ppfcount);

	# Much depends on the assumption that we're called
	# with a ppath that we are "at" in the sizes file due to
	# lookahead.  We initialize late (first call), since no input
	# will be ready for a while.  With luck the input stream will
	# be completely defined by the time we ask for the first line.
	# The line should be of type START or UF; lines of type IN we
	# should have read through until we encounter a line not of
	# type IN (always preceded by START).
	#
	if (! $sztype) {		# lazy initialization step
		($sztype, $which, $size) = getsizeline();
	}

	# Check that the $ppath we're called with matches the current
	# size line.  The check depends on the $sztype.  Remember:
	#	START %h 0	(for types NS and PM)
	#	UF %h %s	(our $ppath _is_ %h)
	#	IN %p %s	(our $ppath is contained in %p)
	# UF can be followed by START at same level (UG)
	# START can be followed by START at same level (UG)
	# xxx all these string comparisons... more efficient with ints?
	die("unexpected size line type: $sztype")
		unless ($sztype eq "START" || $sztype eq "UF");
	die("didn't find $ppath in triple: $sztype, $which, $size")
		if ($which ne $ppath);
	$ppbcount = $size;		# initialize
	$ppfcount = ($sztype eq "UF" ? 1 : 0);
	while (1) {
		($sztype, $which, $size) = getsizeline();

		if ($sztype eq "IN" && $which =~ /^$ppath/) {
			$ppbcount += $size;
		}
		elsif ($sztype eq "START") {
			last if ($which ne $ppath);
		}
		elsif ($sztype eq "UF") {
			last if ($which ne $ppath);
			$ppbcount += $size;
		}
		else {
			die("unexpected triple in size run for $ppath: "
				. "$sztype, $which, $size");
		}
		$ppfcount++;		# another file counted
	}

	# If we're here, we have total size for the given $ppath.
	# Before returning, update the overall byte and file counts
	# for the pairtree.
	#
	$ptbcount += $ppbcount;
	$ptfcount += $ppfcount;

	return "$ppbcount.$ppfcount";

	# xxxx
	#    find $f -type f | sed "s/.*/'&'/" | xargs stat -t | \
	#        awk -v f=$f '{s += $2} END {printf "%s.%s %s\n", s, NR, f}'
}

# xxx get the path right for this file
open(FIX, "> pt_fix") || warn("xxx can't open fix file");

my ($pp, $found, $type, $object);
$pp = $found = $type = $object = '';
my ($prevpp, $prevfound, $prevtype);
my $done = 0;
my $encaperrs = 0;
my $encapoks = 0;
my $emptyppaths = 0;
my $sizestr = '(:unas)';
my $msg = '';
my $verbose = 0;

# Process the 'find' output lines for objects and look for anomalies.
# Can't conclude about unencapsulated objects until we're past the
# object (this requires sort to cluster candidate objects), which means
# that we always know what the previous line and current line have on them.
#
while (1) {
	$prevpp = $pp;
	$prevfound = $found;
	$prevtype = $type;

	$_ = <FIND>;
	if (defined($_)) {
		chomp;
		if (! /^(\S+) (\S+) (.*)/) {
			# a "show all" line; pass thru
			#print "xxx: $_\n";
			next;
		}
		($pp, $type, $found) = ($1, $2, $3);
		print "Verbose: $_\n" if ($verbose);
		if ($type eq "EP") {
			# This is the only type of "found" item that doesn't
			# correspond to an object, so we can deal with it
			# without waiting for the next line to tell us what
			# it is.  We still have to fall through for the
			# sake of what encountering this item means for our
			# deduction about previous line's role, ie, we can't
			# just short cut to the next iteration with 'next;'
			#
			print "null: $pp\n";
			print FIX "null: $pp\n";
			$emptyppaths++;
		}
	}
	else {		# EOF reached -- this will be last time thru loop
		# When EOF is found, want $pp empty for one last run through
		# loop in order to properly eject final line.
		$pp = '';	# want $pp empty for last run
		$_ = '';	# want $_ defined in case of debug print
		$done = 1;
	}

	# Report is one-line per object.  Line format is one of two types
	# ok: id|filename|size|path
	# warn: id|something|size|path|message

	# This is the main part of the loop.  Normally, there would be
	# one line per object, but in the presence of encapsulation errors
	# there will be more than one line having the same ppath.  Because
	# the input was sorted first, we know that any such lines will be
	# clustered in a group, and all we have to do is detect when we
	# enter a group (sharing a ppath) and leave a group.  We do this
	# by processing the current line while remembering the previous
	# line.  There are two states: "in object" ($object ne '') or not
	# "in object" ($object eq '').
	#
	if ($object) {				# if "in object"
		if ($pp eq $prevpp) {		#    and ppath is same
			# stay "in object" and add to existing object
			$object .= " $found";
		}
		else {				# else leave object
			# dump and zero out object in preparation for another
			# xxx write fixit script to create temp dir, move
			# stuff into it, then rename to 'obj'
			$sizestr = getsize($prevpp);
			$msg = "warn: " . ppath2id($prevpp) .
				" | UG $object | " .
				$sizestr . " | $prevpp | " . 
				"unencapsulated file/dir group\n";
			print $msg;
			print FIX $msg;
			$object = "";
			$encaperrs++;
		}
	}
	else {					# if not "in object"
		#print "pp=$pp, prevpp=$prevpp, $_\n";
		if ($pp eq $prevpp) {		#    and ppath is same
			# then start new object
			$object = "$prevfound $found";
		}
		# else not entering an object; check UF and PM cases
		elsif ($prevtype eq "UF" || $prevtype eq "PM") {
			# offer to encapusulate a lone file
			$sizestr = getsize($prevpp);
			$msg = "warn: " . ppath2id($prevpp) .
				" | $prevtype $prevfound | " .
				$sizestr . " | $prevpp | " .
				($prevtype eq "UF" ? "unencapsulated file" :
				    "encapsulating directory name too short")
				. "\n";
			print $msg;
			print FIX $msg;
			$encaperrs++;
		}
		# else in mainstream case, except line 1 ($prevtype eq '')
		# XXX explain why EP needs to run through and can't
		# short cut "next" the loop
		elsif ($prevtype && $prevtype ne "EP") {
			$sizestr = getsize($prevpp);
			print "ok: ", ppath2id($prevpp), " | $prevfound | ",
				$sizestr, " | $prevpp\n";
			$encapoks++;
		}
	}
	last
		if ($done);
}

close(FIND);
close(FIX);
close(SIZES);

my $objcount = $encapoks + $encaperrs;

print "$objcount objects, including $encaperrs encapsulation warnings.  ";
print "There are $emptyppaths empty pairpaths\n";

# XXXX make sure to declare/catch /be/nt/o/r/ as improper encapsulation
