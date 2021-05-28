To generate PDF, make sure you have a recent version of either [TeX Live](https://www.tug.org/texlive/) or [MacTeX](https://www.tug.org/mactex/). Then, 

```
xelatex bmc_artcile
bibtex bmc_article
xelatex bmc_article
```

You might have to run the command a couple of times to resolve references. If you're using MacTeX, `xelatex` lives under `/Library/TeX/texbin`, so make sure this path is in your `PATH` environment.
