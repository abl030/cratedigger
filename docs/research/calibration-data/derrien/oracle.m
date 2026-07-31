% Drive Derrien's UNMODIFIED detect_aac.m / init_aac.m / MDCT.m over a fixed
% set of offsets, so the numpy port can be checked against the reference.
args = argv();
wavfile = args{1};
outfile = args{2};
offsets = str2num(args{3});
frames  = str2num(args{4});

[signal, Fs] = audioread(wavfile);
Fs = round(Fs/1000);
signal_L = signal(:,1); signal_R = signal(:,2);
signal_M = signal_L + signal_R;

[nb_short_win, iblen_long, iblen_short, offset_short, ...
 nb_sb_long, nb_sb_short, w_low_swb_long, w_high_swb_long, ...
 w_low_swb_short, w_high_swb_short, wn_long_sin, wn_start_sin, ...
 wn_stop_sin, wn_short_sin] = init_aac(Fs);

ref_proba = 1e-2;
swb_width_long = w_high_swb_long - w_low_swb_long + 1;
swb_width_short = (w_high_swb_short - w_low_swb_short + 1)*nb_short_win;
mu = 1/12;
sigma_long = sqrt(1./swb_width_long/180);
sigma_short = sqrt(1./swb_width_short/180);
tau_sb_long = mu - sqrt(2)*sigma_long.*erfinv(erf(mu./sigma_long/sqrt(2))-2*ref_proba);
tau_sb_short = mu - sqrt(2)*sigma_short.*erfinv(erf(mu./sigma_short/sqrt(2))-2*ref_proba);

nb_sf = 8; sf_min = 0.3; sf_max = 0.7;
sf = sf_min:(sf_max-sf_min)/(nb_sf-1):sf_max;

fid = fopen(outfile, 'w');
for i = 1:length(offsets)
    p = detect_aac(signal_M, offsets(i), sf, frames, ...
        iblen_long, iblen_short, offset_short, nb_sb_long, nb_sb_short, nb_short_win, ...
        w_low_swb_long, w_high_swb_long, w_low_swb_short, w_high_swb_short, wn_long_sin, ...
        wn_start_sin, wn_stop_sin, wn_short_sin, tau_sb_long, tau_sb_short);
    fprintf(fid, '%d\t%.15g\n', offsets(i), p);
end
fclose(fid);
