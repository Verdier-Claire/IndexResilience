# IndexResilience
Ce code permet d'obtenir trois indicateurs de résilience par entreprise. <br />

Les jeux de données utilisés dans ce code sont disponibles via le lien suivant :
https://drive.open.studio/s/bigT4QRyaceL3pe/download


## Indice de diversification de la production d'une entreprise
Soit e une entreprise. $n_e$ représente le nombre de produits fabriqués dans cette entreprise. <br />
On note $λ_{p_i}$ le poids du produit $p_i$. <br />
Soit $L_H$ l'ensemble des produits. <br />
On pose $\phi(p,q)$ la fonction de dissimilarité entre le produit p et le produit q. <br />
On peut définir la diversification de la production d'une entreprise de la manière suivante : <br />

$$ACP(e) = \sum_{i \in \{1, 2, ..., n_E\}} \lambda_{p_i} \sum_{q \in L_H} \frac{1 - \phi(p_i, q)}{\sum_r \in L_H} 1 - \phi(q,r)$$


## Indice de la polyvalence de la main-d'œuvre d'une entreprise
Soit e une entreprise. <br />
Soient M l'ensemble des métiers et $M_e$ l'ensemble des métiers de l'entreprise e. <br />
On pose $\lambda_i$ la proportion du métier i liée au code ROME dans le cadre du code NAF de l'entreprise e par rapport
à l'ensemble des métiers présents. <br />
On pose $d(.,.)$ la fonction de distance entre deux métiers calculés par pôle emploie. <br />
On peut définir la polyvalence de la main-d'œuvre de l'entreprise e de la manière suivante : <br />
$$VCW(e)= \sum_{i \in M_e} \lambda_i \sum_{j \in M} d(i,j) max(1, \sum_{l \in M} x_{jl} )$$

## Indice des fournisseurs locaux de secours
Soit E l'ensemble des entreprises en France. 
Soit e une entreprise. 
On pose $dist(.,.)$ la distance euclidienne entre deux entreprises par rapport à leurs coordonnées gps. <br />
Soit $\alpha_i$ le secteur d'activité d'un des fournisseurs de l'entreprise e. <br />
On pose $E_{\alpha_i, \delta}^e$ l'ensemble des entreprises qui sont des potentiels fournisseurs de l'entreprise e 
dans un rayon de $\delta$ km. 
On peut définir l'indice des fournisseurs de secours locaux de la manière suivante : <br /> 
$$LBS(e) = \frac{\sum_{i \in \{1, 2, ..., n_E\}}  \lambda_{\alpha_i}^e max (1, |E_{\alpha_i, \delta}^e|}{\sum_{\tilde{e} \in E } \lambda_{\alpha_i}^e 1_{\tilde{e} \text{a comme secteur d'activites de fournisseurs } \alpha_i} 1_{dist(e,\tilde{e}) \leq \delta}}$$


## Indice de la résilience des flux d'approvisionnements
Soit e une entreprise. <br /> 
Soit $A^e$ l'ensemble des activités des fournisseurs de l'entreprise e. <br />
Soit $P_{\alpha}$ la liste des produits de l'activité $\alpha$. <br />
Soit $\lambda_{\alpha}$ la consommation intermédiaire pour l'activité $\alpha$. <br /> 
Soit $l_p$ le poids du produit p dans l'activité $\alpha$. <br />
Soit $r_p$ la résilience du produit p. <br />
La résilience du produit p, notée $r_p$ est définie de la manière suivante : 
$$r_p = - \alpha \text{log} \alpha$$
Où : <br />
$$\alpha = \frac{\text{Productivité}}{\text{Productivité} + \text{Redondance} }$$
$$\text{Redondance} = - \sum_{i,j} \frac{T_{ij}}{T_{..}} \text{log} \frac{T^2_{ij}}{T_{i.}T_{.j}}$$
$$\text{Productivité} = \sum_{i,j} \frac{T_{ij}}{T_{..}} \text{log} \frac{T_{ij}T_{..}}{T_{i.}T_{.j}}$$

On peut ainsi définir l'indice de la résilience des flux d'approvisionnements de la façon suivante : <br />
$$RSF(e) = \sum_{\alpha \in A^e} \lambda_{\alpha} \sum_{p \in P_{\alpha}} l_p r_p$$
